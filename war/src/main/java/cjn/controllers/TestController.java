package cjn.controllers;

import cjn.support.TestService;
import org.apache.spark.SparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

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
