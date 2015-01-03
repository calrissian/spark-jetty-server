package cjn.controllers;

import cjn.support.Service;
import org.apache.spark.SparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller(value= "/test")
public class TestController {

  private final SparkContext sparkContext;

  @Autowired
  public TestController(SparkContext sparkContext) {

    System.out.println("INIT CONTROLLER!");
    this.sparkContext = sparkContext;
  }

  @RequestMapping(value = "/", method = RequestMethod.GET)
  @ResponseBody
  public String test() {
    return Service.test(sparkContext);
  }
}
