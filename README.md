# Spark Jetty Server

This is a simple example demonstrating how to embed a ```SparkContext``` within a Jetty web server running in yarn-client mode.
This proved to be non-trivial as an understanding of how the Spark classpath is built is quite necessary to make this work. So
far, this has only been tested using the Jetty maven plugin, but it should translate fairly easily to an actual jetty instance.

This project depends on YARN being installed and configured. Spark needs to be installed and configured as well. You will also
need to set the appropriate environment variables for these apps: SPARK_HOME and HADOOP_HOME (or HADOOP_PREFIX).

## Building and running

1. Build the project from the root using ```mvn clean install```.

2. ```cd``` into the ```war/``` directory and run ```mvn jetty:run```.

3. Once the webserver is up and running, navigate to ```http://localhost:8080/test``` and watch the result of a quick job.


Hopefully this will open up new doors for implementing different use-cases for Spark.
