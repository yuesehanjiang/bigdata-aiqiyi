package com.study.sparkweb;

import org.springframework.web.bind.annotation.Mapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

/**
 * Created by zhang on 2017/11/22.
 */
@RestController
public class HelloWeb {

    @RequestMapping(value = "hello", method = RequestMethod.GET)
    public String hello() {
        return "hellospoot";
    }

    @RequestMapping(value = "/first", method = RequestMethod.GET)
    public ModelAndView firstDemo() {
        System.out.print("11111111111");
        return new ModelAndView("test");
    }
}
