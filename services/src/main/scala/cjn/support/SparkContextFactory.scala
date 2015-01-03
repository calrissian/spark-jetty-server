package cjn.support

import org.apache.spark.{SparkContext, SparkConf}

object SparkContextFactory {


  def newSparkContext: SparkContext = {

    val sparkAssemblyJar = sys.props.get("spark.assembly.jar").getOrElse("")

    val sparkConf = new SparkConf().setJars(Array(
      SparkContext.jarOfClass(this.getClass).get,
      sparkAssemblyJar
    ))

    new SparkContext("yarn-client", "TestSparkWebApp", sparkConf)
  }
}
