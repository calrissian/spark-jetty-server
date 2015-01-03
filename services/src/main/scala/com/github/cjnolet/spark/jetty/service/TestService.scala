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
package com.github.cjnolet.spark.jetty.service

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
