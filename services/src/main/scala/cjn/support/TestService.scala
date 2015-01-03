package cjn.support

import org.apache.spark.SparkContext

/**
 * A stateless service class that interacts with a spark context.
 * @param sparkContext
 */
class TestService(sparkContext: SparkContext) {

  /**
   * A simple count of items in a sequence which is able to be processed in parallel
   */
  def test: String = sparkContext.parallelize(0 to 500000, 25).count.toString
}
