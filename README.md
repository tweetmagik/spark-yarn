# Spark-Yarn

`spark-yarn` launches Spark applications on YARN, the resource manager for the Hadoop MapReduce 2.0
project. You specify a JAR for the application, the locations where Mesos and Spark are installed
on the cluster, and the number of slave resources to acquire, and `spark-yarn` will launch a cluster
with the required resources and run your job.

## Building

To build `spark-yarn`, you will need Maven 3 or higher. Then run:

    mvn package

In addition, you must build Mesos and Spark on your cluster nodes. Please refer to
<https://github.com/mesos/mesos/wiki> and <https://github.com/mesos/spark/wiki> for details.

## Running

The main script to run the project is `bin/client`. Execute this without arguments to see
a list of the available options. At the very least, you will need to pass the following options:

* `-slaves`: number of slave nodes to acquire
* `-mesos_home`: location where Mesos is installed on cluster nodes
* `-spark_home`: location where Spark is installed on cluster nodes
* `-jar`: JAR containing your code
* `-class`: main class to execute from your JAR.

In addition, you should set the `HADOOP_CONF_DIR` environment variable to refer to your
Hadoop/YARN installation's config directory.

## Viewing Output from your Job

`spark-yarn` places log files in the standard YARN log directory for applications. There will
be multiple files with the standard output and error streams of various processes.
While the application is running, you can view these logs by clicking the "container logs"
links on the YARN web UI. After the application ends, the links no longer work, and you should
look for the logs directly in your file system. By default they get placed in `/tmp/logs`.
