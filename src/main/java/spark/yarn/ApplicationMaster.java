package spark.yarn;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.util.Records;

import com.google.common.collect.Lists;

public class ApplicationMaster {
  private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

  private ApplicationAttemptId appAttemptId;
  private Configuration conf;
  private YarnRPC rpc;
  private AMRMProtocol resourceManager;
  private int responseId = 0; // For identifying resource requests and responses

  // Have a cache/map of UGIs so as to avoid creating too many RPC
  // client connection objects to the same NodeManager
  private Map<String, UserGroupInformation> ugiMap = new HashMap<String, UserGroupInformation>();
  
  public static void main(String[] args) throws Exception {
	new ApplicationMaster(args).run();
  }

  public ApplicationMaster(String[] args) {
	// Get our application attempt ID from the command line arguments
	ApplicationId appId = Records.newRecord(ApplicationId.class);
    appId.setClusterTimestamp(Long.valueOf(args[0]));
    appId.setId(Integer.valueOf(args[1]));
    int failCount = Integer.valueOf(args[2]);
    LOG.info("Application ID: " + appId + ", fail count: " + failCount);
    appAttemptId = Records.newRecord(ApplicationAttemptId.class);
    appAttemptId.setApplicationId(appId);
    appAttemptId.setAttemptId(failCount);
    
    // Set up our configuration and RPC
	conf = new Configuration();
	rpc = YarnRPC.create(conf);
  }

  private void run() throws IOException {
	LOG.info("Starting app master");
	resourceManager = connectToRM();
    registerWithRM();
	
    int totalTasks = 2;
    int tasksLaunched = 0;
    int tasksFinished = 0;
    boolean sentRequest = false;
    
    List<ResourceRequest> noRequests = new ArrayList<ResourceRequest>();
    List<ContainerId> noReleases = new ArrayList<ContainerId>();
    
    while (tasksFinished != totalTasks) {
      AMResponse response;
      if (!sentRequest) {
        sentRequest = true;
        ResourceRequest request = createRequest(totalTasks);
        LOG.info("Making resource request: " + request);
        response = allocate(Lists.newArrayList(request), noReleases);
      } else {
    	response = allocate(noRequests, noReleases);
      }
      System.out.println("AM response: " + response);
      for (Container container: response.getNewContainerList()) {
    	launchContainer(container);
    	tasksLaunched++;
      }
      for (Container container: response.getFinishedContainerList()) {
    	LOG.info("Container finished: " + container);
    	tasksFinished++;
      }
      
  	  try {
  	    Thread.sleep(1000);
      } catch (InterruptedException e) {
  	    e.printStackTrace();
      }
    }

    LOG.info("Shutting down");
	unregister();
  }

  private void launchContainer(Container container) throws IOException {
	ContainerManager mgr = connectToCM(container);
	ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
	ctx.setContainerId(container.getId());
	ctx.setResource(container.getResource());
	ctx.setUser(UserGroupInformation.getCurrentUser().getShortUserName());
	ctx.addCommand("/bin/sleep 60");
	StartContainerRequest req = Records.newRecord(StartContainerRequest.class);
	req.setContainerLaunchContext(ctx);
	mgr.startContainer(req);
  }

  private ContainerManager connectToCM(Container container) throws IOException {
	// Based on similar code in the ContainerLauncher in Hadoop MapReduce
	ContainerToken contToken = container.getContainerToken();
	final String address = container.getNodeId().getHost() + ":" + container.getNodeId().getPort();
	LOG.info("Connecting to container manager for " + address);
	UserGroupInformation user = UserGroupInformation.getCurrentUser();
	if (UserGroupInformation.isSecurityEnabled()) {
	  if (!ugiMap.containsKey(address)) {
		Token<ContainerTokenIdentifier> hadoopToken = 
		  new Token<ContainerTokenIdentifier>(
			contToken.getIdentifier().array(), contToken.getPassword().array(),
			new Text(contToken.getKind()), new Text(contToken.getService()));
		user = UserGroupInformation.createRemoteUser(address);
		user.addToken(hadoopToken);
		ugiMap.put(address, user);
	  } else {
		user = ugiMap.get(address);
	  }
	}
	ContainerManager mgr = user.doAs(new PrivilegedAction<ContainerManager>() {
	  public ContainerManager run() {
		YarnRPC rpc = YarnRPC.create(conf);
		return (ContainerManager) rpc.getProxy(ContainerManager.class,
		    NetUtils.createSocketAddr(address), conf);
	  }
	});
	return mgr;
  }

  private ResourceRequest createRequest(int totalTasks) {
	ResourceRequest request = Records.newRecord(ResourceRequest.class);
    request.setHostName("*");
    request.setNumContainers(totalTasks);
    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(1);
    request.setPriority(pri);
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(1024);
    request.setCapability(capability);
	return request;
  }

  private AMRMProtocol connectToRM() {
	InetSocketAddress rmAddress = NetUtils.createSocketAddr(conf.get(
	    YarnConfiguration.SCHEDULER_ADDRESS,
	    YarnConfiguration.DEFAULT_SCHEDULER_BIND_ADDRESS));
	LOG.info("Connecting to ResourceManager at " + rmAddress);
	return ((AMRMProtocol) rpc.getProxy(AMRMProtocol.class, rmAddress, conf));
  }

  private void registerWithRM() throws YarnRemoteException {
	RegisterApplicationMasterRequest req =
      Records.newRecord(RegisterApplicationMasterRequest.class);
    req.setApplicationAttemptId(appAttemptId);
    req.setHost("");
    req.setRpcPort(1);
    req.setTrackingUrl("");
	resourceManager.registerApplicationMaster(req);
	LOG.info("Successfully registered with resource manager");
  }

  private AMResponse allocate(List<ResourceRequest> resourceRequest, List<ContainerId> releases)
	  throws YarnRemoteException {
	AllocateRequest req = Records.newRecord(AllocateRequest.class);
	req.setResponseId(++responseId);
	req.setApplicationAttemptId(appAttemptId);
	req.addAllAsks(resourceRequest);
	req.addAllReleases(releases);
	AllocateResponse resp = resourceManager.allocate(req);
	return resp.getAMResponse();
  }

  private void unregister() throws YarnRemoteException {
	LOG.info("Unregistering application");
    FinishApplicationMasterRequest req =
      Records.newRecord(FinishApplicationMasterRequest.class);
    req.setAppAttemptId(appAttemptId);
    req.setFinalState("SUCCEEDED");
    resourceManager.finishApplicationMaster(req);
  }
}
