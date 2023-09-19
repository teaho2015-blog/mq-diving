package net.teaho.demo.spring.rocketmq.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author teaho2015@gmail.com
 * @date 2023-06
 */
@RestController
public class TestController {


	@GetMapping("/hello")
	public String hello() {
		return "Hello";
	}

}
