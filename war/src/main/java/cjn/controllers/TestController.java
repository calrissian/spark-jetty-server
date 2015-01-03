package cjn.controllers;

import cjn.support.TestService;
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

  @Autowired
  public TestController(SparkContext sparkContext) {
    this.testService = new TestService(sparkContext);
  }

  @RequestMapping(value = "/", method = RequestMethod.GET)
  @ResponseBody
  public String test() {
    return testService.test();
  }
}
