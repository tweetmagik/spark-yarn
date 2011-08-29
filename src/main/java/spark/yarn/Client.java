package spark.yarn;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.URI;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

  private String mesosHome;
  private String sparkHome;
  private int numSlaves;
  private int slaveMem;
  private int slaveCpus;
  private int masterMem;
  private String jar;
  private String mainClass;
  private String programArgs;
  
  private Configuration conf;
  private YarnRPC rpc;
  private ClientRMProtocol applicationsManager;
  private URI jarUri;

  public Client(String[] args) {
    try {
      Options opts = new Options();
      opts.addOption("mesos_home", true, "directory where Mesos is installed");
      opts.addOption("spark_home", true, "directory where Spark is installed");
      opts.addOption("slaves", true, "number of slaves");
      opts.addOption("slave_mem", true, "memory per slave, in MB (default: 1024)");
      opts.addOption("master_mem", true, "memory for master, in MB (default: 1024)");
      opts.addOption("slave_cpus", true, "CPUs to use per slave (default: 1)");
      opts.addOption("jar", true, "JAR file containing job");
      opts.addOption("class", true, "main class to run (default: look at JAR manifest)");
      opts.addOption("args", true, "arguments to pass to main class");
      opts.addOption("help", false, "print this help message");
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
      if ((jar = line.getOptionValue("jar")) == null) {
        System.err.println("Application JAR needs to be specified, using -jar");
        System.exit(1);
      }
      slaveMem = Integer.parseInt(line.getOptionValue("slave_mem", "1024"));
      slaveCpus = Integer.parseInt(line.getOptionValue("slave_cpus", "1"));
      masterMem = Integer.parseInt(line.getOptionValue("master_mem", "1024"));
      mainClass = line.getOptionValue("class", "");
      programArgs = line.getOptionValue("args", "");
      LOG.info("programArgs: " + programArgs);
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

    ApplicationId appId = newApplicationId();
    LOG.info("Got application ID " + appId);

    // Upload the job's JAR to HDFS 
    FileSystem fs = FileSystem.get(conf);
    Path stagingDir = Utils.getStagingDir(conf, appId);
    Path destPath = new Path(stagingDir, "job.jar");
    jarUri = fs.getUri().resolve(destPath.toString());
    LOG.info("Uploading job JAR to " + jarUri);
    fs.copyFromLocalFile(new Path(jar), destPath);

    ApplicationSubmissionContext ctx = createApplicationSubmissionContext(conf, appId);
    
    // Launch the application master
    SubmitApplicationRequest request = Records.newRecord(SubmitApplicationRequest.class);
    request.setApplicationSubmissionContext(ctx);
    applicationsManager.submitApplication(request);
    LOG.info("Submitted application to ResourceManager");
  }

  private ApplicationSubmissionContext createApplicationSubmissionContext(
      Configuration conf, ApplicationId appId) throws Exception {
    ApplicationSubmissionContext ctx = Records.newRecord(ApplicationSubmissionContext.class);
    ctx.setApplicationId(appId);
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(masterMem);
    LOG.info("AppMaster capability = " + capability);
    ctx.setMasterCapability(capability);

    // Add job name
    ctx.setApplicationName("Spark");

    // Add command line
    String home = System.getenv("SPARK_YARN_HOME");
    if (home == null)
      home = new File(".").getAbsolutePath();
    ctx.addCommand(home + "/bin/application-master " +
        "-yarn_timestamp " + appId.getClusterTimestamp() + " " +
        "-yarn_id " + appId.getId() + " " +
        "-yarn_fail_count " + ApplicationConstants.AM_FAIL_COUNT_STRING + " " +
        "-mesos_home " + Utils.quote(mesosHome) + " " +  
        "-spark_home " + Utils.quote(sparkHome) + " " +
        "-slaves " + numSlaves + " " +
        "-slave_mem " + slaveMem + " " +
        "-slave_cpus " + slaveCpus + " " +
        "-master_mem " + masterMem + " " +
        "-jar_uri " + jarUri + " " +
        "-class " + Utils.quote(mainClass) + " " +
        (programArgs.equals("") ? "" : "-args " + Utils.quote(programArgs) + " ") +
        "-log_dir " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + " " +
        "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout " +
        "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

    // TODO: RM should get this from RPC
    ctx.setUser(UserGroupInformation.getCurrentUser().getShortUserName());
    return ctx;
  }

  private ApplicationId newApplicationId() throws YarnRemoteException {
    GetNewApplicationIdRequest request = Records.newRecord(GetNewApplicationIdRequest.class);
    return applicationsManager.getNewApplicationId(request).getApplicationId();
  }
}
