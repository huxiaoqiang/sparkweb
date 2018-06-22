package com.huxiaoqiang.sparkweb;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableAutoConfiguration
public class SparkwebApplication {

    public static void main(String[] args) {
        SpringApplication.run(SparkwebApplication.class, args);
    }
}