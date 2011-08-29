package spark.yarn;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;

public class Utils {
  /**
   * Quote and escape a string for use in shell commands.
   */
  public static String quote(String str) {
    return "\"" + str.replaceAll("\\\\", "\\\\\\\\").replaceAll("\"", "\\\\\"") + "\"";
  }

  public static Path getStagingDir(Configuration conf, ApplicationId appId) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    Path stagingDir = new Path(conf.get("yarn.apps.stagingDir", "/tmp/yarn-staging") + 
        Path.SEPARATOR + user + Path.SEPARATOR + appId + Path.SEPARATOR + "staging");
    if (!fs.exists(stagingDir)) {
      fs.mkdirs(stagingDir);
    }
    return stagingDir;
  }
}
