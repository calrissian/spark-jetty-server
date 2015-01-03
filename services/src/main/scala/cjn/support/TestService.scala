package cjn.support

import org.apache.spark.SparkContext

/**
 * A stateless service class that interacts with a spark context.
 * @param sparkContext
 */
class TestService(sparkContext: SparkContext) {

  def test: String = sparkContext.parallelize(0 to 500000).count.toString
}
