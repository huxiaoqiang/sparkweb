package com.huxiaoqiang.sparkweb.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping(value = {"/","index"})
public class IndexController {

    public String index(){
        return "index";
    }
}