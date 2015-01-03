package cjn.support

import org.apache.spark.SparkContext


object Service {

  def test(@transient sparkContext: SparkContext): String = sparkContext.parallelize(0 to 500000).count.toString

}
