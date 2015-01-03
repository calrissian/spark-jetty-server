package cjn.support

import org.apache.spark.{SparkContext, SparkConf}

/**
 * A simple factory to create a SparkContext.
 */
object SparkContextFactory {


  /**
   * This method is responsible for shipping jars off to the executors and setting any base state on the
   * SparkConf. It's important that we ship off our services jar so that the executors can access any
   * classes we've created.
   */
  def newSparkContext: SparkContext = {

    /**
     * Just for simplicity, the spark home and the spark yarn uber-jar are coming in through system properties
     */
    val sparkHome = sys.props.get("spark.home").getOrElse("")

    val sparkConf = new SparkConf()
      .setJars(Array(SparkContext.jarOfClass(this.getClass).get))
      .setSparkHome(sparkHome)

    new SparkContext("yarn-client", "TestSparkJettyServer", sparkConf)
  }
}
