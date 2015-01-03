package cjn.support

import org.apache.spark.{SparkContext, SparkConf}

object SparkContextFactory {


  def newSparkContext(): SparkContext = {
    val sparkConf = new SparkConf().setJars(Array(
      SparkContext.jarOfClass(this.getClass).get,
      "/Library/Spark/spark-current/lib/spark-assembly-1.1.0-hadoop2.4.0.jar"
    ))

    val sc = new SparkContext("yarn-client", "AppName", sparkConf)

    println(sc.parallelize(0 to 50000).count)

    sc
  }
}
