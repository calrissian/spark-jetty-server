/*
 * Copyright (C) 2014 Corey J. Nolet
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.cjnolet.spark.jetty.support

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
     * Just for simplicity, the spark home is coming in through a system property
     */
    val sparkHome = sys.props.get("spark.home").getOrElse("")

    val sparkConf = new SparkConf()
      .setJars(Array(SparkContext.jarOfClass(this.getClass).get))
      .setSparkHome(sparkHome)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.scheduler.mode", "FAIR")

    new SparkContext("yarn-client", "TestSparkJettyServer", sparkConf)
  }
}
