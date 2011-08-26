package spark.yarn;

import java.io.File;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationIdRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.client.ClientRMSecurityInfo;
import org.apache.hadoop.yarn.util.Records;

class Client {

  private static final Log LOG = LogFactory.getLog(Client.class);

  private Configuration conf;
  private YarnRPC rpc;
  private ClientRMProtocol applicationsManager;

  public Client(String[] args) {
  }

  public static void main(String[] args) throws Exception {
	new Client(args).run();
  }

  public void run() throws Exception {
	conf = new Configuration();
	rpc = YarnRPC.create(conf);
	InetSocketAddress rmAddress = NetUtils.createSocketAddr(conf.get(
	    YarnConfiguration.APPSMANAGER_ADDRESS,
	    YarnConfiguration.DEFAULT_APPSMANAGER_BIND_ADDRESS));
	LOG.info("Connecting to ResourceManager at " + rmAddress);
	Configuration appsManagerServerConf = new Configuration(conf);
	appsManagerServerConf.setClass(YarnConfiguration.YARN_SECURITY_INFO,
	    ClientRMSecurityInfo.class, SecurityInfo.class);
	applicationsManager = (ClientRMProtocol) rpc.getProxy(
	    ClientRMProtocol.class, rmAddress, appsManagerServerConf);

	ApplicationSubmissionContext appContext = createApplicationSubmissionContext(conf);

	SubmitApplicationRequest request = Records.newRecord(SubmitApplicationRequest.class);
	request.setApplicationSubmissionContext(appContext);
	applicationsManager.submitApplication(request);
	LOG.info("Submitted application to ResourceManager");
  }

  private ApplicationSubmissionContext createApplicationSubmissionContext(
	  Configuration jobConf) throws Exception {
	ApplicationSubmissionContext appContext = Records.newRecord(ApplicationSubmissionContext.class);
	ApplicationId applicationId = newApplicationId();
	LOG.info("Got application ID " + applicationId);
	appContext.setApplicationId(applicationId);
	Resource capability = Records.newRecord(Resource.class);
	capability.setMemory(1024);
	LOG.info("AppMaster capability = " + capability);
	appContext.setMasterCapability(capability);

	// Add job name
	appContext.setApplicationName("Spark");

    // Add command line
	String home = System.getenv("SPARK_YARN_HOME");
	if (home == null)
	  home = new File(".").getAbsolutePath();
	appContext.addCommand(home + "/bin/application-master " +
		applicationId.getClusterTimestamp() + " " + 
		applicationId.getId() + " " +
		ApplicationConstants.AM_FAIL_COUNT_STRING + " " +
		"1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout " +
		"2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

	// TODO: RM should get this from RPC.
	appContext.setUser(UserGroupInformation.getCurrentUser().getShortUserName());
	return appContext;
  }

  private ApplicationId newApplicationId() throws YarnRemoteException {
	GetNewApplicationIdRequest request = Records.newRecord(GetNewApplicationIdRequest.class);
	return applicationsManager.getNewApplicationId(request).getApplicationId();
  }
}
