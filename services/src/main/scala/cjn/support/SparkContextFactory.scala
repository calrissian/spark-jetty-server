package cjn.support

import org.apache.spark.{SparkContext, SparkConf}

object SparkContextFactory {


  def newSparkContext: SparkContext = {

    val sparkAssemblyJar = sys.props.get("spark.assembly.jar").getOrElse("")
    val sparkHome = sys.props.get("spark.home").getOrElse("")

    val sparkConf = new SparkConf()
      .setJars(Array(SparkContext.jarOfClass(this.getClass).get, sparkAssemblyJar))
      .setSparkHome(sparkHome)


    new SparkContext("yarn-client", "TestSparkWebApp", sparkConf)
  }
}
