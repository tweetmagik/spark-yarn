package spark.yarn;

import java.io.File;
import java.net.InetSocketAddress;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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
  private String mesosHome;
  private String sparkHome;
  private int numSlaves;
  private int memory;
  private int cpus;

  public Client(String[] args) {
    try {
      Options opts = new Options();
      opts.addOption("mesos_home", true, "Directory where Mesos is installed");
      opts.addOption("spark_home", true, "Directory where Spark is installed");
      opts.addOption("slaves", true, "Number of slaves");
      opts.addOption("memory", true, "Memory per slave, in MB (default: 1024)");
      opts.addOption("cpus", true, "CPUs to use per slave (default: 1)");
      opts.addOption("help", false, "Print this help message");
      CommandLine line = new GnuParser().parse(opts, args);
      
      if (args.length == 0 || line.hasOption("help")) {
        new HelpFormatter().printHelp("client", opts);
        System.exit(0);
      }
      if ((mesosHome = line.getOptionValue("mesos_home", System.getenv("MESOS_HOME"))) == null) {
        System.err.println("MESOS_HOME needs to be specified, either as a command line " +
            "option or through the environment");
        System.exit(1);
      }
      if ((sparkHome = line.getOptionValue("spark_home", System.getenv("SPARK_HOME"))) == null) {
        System.err.println("SPARK_HOME needs to be specified, either as a command line " +
            "option or through the environment");
        System.exit(1);
      }
      if ((numSlaves = Integer.parseInt(line.getOptionValue("slaves", "-1"))) == -1) {
        System.err.println("Number of slaves needs to be specified, using -slaves");
        System.exit(1);
      }
      memory = Integer.parseInt(line.getOptionValue("memory", "1024"));
      cpus = Integer.parseInt(line.getOptionValue("cpus", "1"));
    } catch (ParseException e) {
      System.err.println("Failed to parse command line options: " + e.getMessage());
      System.exit(1);
    }
  }

  public static void main(String[] args) throws Exception {
    new Client(args).run();
  }

  public void run() throws Exception {
    conf = new Configuration();
    rpc = YarnRPC.create(conf);
    
    InetSocketAddress rmAddress = NetUtils.createSocketAddr(
        conf.get(YarnConfiguration.APPSMANAGER_ADDRESS, YarnConfiguration.DEFAULT_APPSMANAGER_BIND_ADDRESS));
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    Configuration appsManagerServerConf = new Configuration(conf);
    appsManagerServerConf.setClass(YarnConfiguration.YARN_SECURITY_INFO,
        ClientRMSecurityInfo.class, SecurityInfo.class);
    applicationsManager = 
      (ClientRMProtocol) rpc.getProxy(ClientRMProtocol.class, rmAddress, appsManagerServerConf);

    ApplicationSubmissionContext appContext = createApplicationSubmissionContext(conf);

    SubmitApplicationRequest request = Records.newRecord(SubmitApplicationRequest.class);
    request.setApplicationSubmissionContext(appContext);
    applicationsManager.submitApplication(request);
    LOG.info("Submitted application to ResourceManager");
  }

  private ApplicationSubmissionContext createApplicationSubmissionContext(Configuration jobConf)
      throws Exception {
    ApplicationSubmissionContext appContext = Records.newRecord(ApplicationSubmissionContext.class);
    ApplicationId appId = newApplicationId();
    LOG.info("Got application ID " + appId);
    appContext.setApplicationId(appId);
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
        "-yarn_timestamp " + appId.getClusterTimestamp() + " " +
        "-yarn_id " + appId.getId() + " " +
        "-yarn_fail_count " + ApplicationConstants.AM_FAIL_COUNT_STRING + " " +
        "-mesos_home \"" + mesosHome + "\" " +  
        "-spark_home \"" + sparkHome + "\" " +
        "-slaves " + numSlaves + " " +
        "-memory " + memory + " " +
        "-cpus " + cpus + " " +
        "-log_dir " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + " " +
        "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout " +
        "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

    // TODO: RM should get this from RPC
    appContext.setUser(UserGroupInformation.getCurrentUser().getShortUserName());
    return appContext;
  }

  private ApplicationId newApplicationId() throws YarnRemoteException {
    GetNewApplicationIdRequest request = Records.newRecord(GetNewApplicationIdRequest.class);
    return applicationsManager.getNewApplicationId(request).getApplicationId();
  }
}
