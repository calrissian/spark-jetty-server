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
package org.calrissian.spark.jetty.controllers;

import org.calrissian.spark.jetty.service.TestService;
import org.apache.spark.SparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * A simple controller demonstrating the calling of a service method which will launch a spark job and return
 * the results to the client. In a real web application, it would probably be best to use cometd or some means
 * of returning the results asynchronously as the job times could outlive request timeouts.
 */
@Controller(value= "/test")
public class TestController {

  private final TestService testService;
  private final SparkContext sparkContext;

  @Autowired
  public TestController(SparkContext sparkContext) {
    this.sparkContext = sparkContext;
    this.testService = new TestService(sparkContext);
  }

  @RequestMapping(value = "/", method = RequestMethod.GET)
  @ResponseBody
  public String test() {
    return testService.test();
  }


    @RequestMapping(value = "/schema", method = RequestMethod.GET)
    @ResponseBody
    public String testSchema() {
        return testService.testSchemaInference().prettyJson();
    }

}
