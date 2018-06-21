package com.huxiaoqiang.sparkweb.controller;

import com.alibaba.fastjson.JSONObject;
import org.springframework.web.bind.annotation.*;

import org.apache.spark.deploy.SparkSubmit;

@RestController
@RequestMapping(value = "/spark/")
public class SparkController {

    @RequestMapping(value = "/submit", method = {RequestMethod.POST})
    @ResponseBody
    public String submit(@RequestParam(value = "param") String param) {
        JSONObject jsonObject = JSONObject.parseObject(param);

        String name = "";
        String jarClass = "cn.edu.tsinghua.thssbpm.Main";
        String jar = "";

        String algorithm = jsonObject.getString("algorithm");
        String filePath = jsonObject.getString("input_log");
        String outFilePath = jsonObject.getString("output");
        String driverMemory = jsonObject.getString("driver_memory") + "G";
        String totalExecutorCores = jsonObject.getString("total_executor_cores");
        String executorMemory = jsonObject.getString("executor_memory") + "G";
        switch (algorithm) {
            case "Spark Aplha Miner":
                name = "Spark Alpha Miner";
                jar = "/home/ubuntu/IdeaProjects/SparkAlphaMiner/target/com.huxiaoqiang-1.0-SNAPSHOT.jar";
                break;
            case "Spark Flexible Heuristic Mine":
                name = "Spark Flexible Heuristic Mine";
                jar = "/home/ubuntu/IdeaProjects/SparkFHM/target/Spark-FHM-1.0-SNAPSHOT.jar";
                break;
        }

        String[] SubmitString = new String[]{
                "--master", "spark://166.111.80.15:7077",
                "--name", name,
                "--executor-memory", executorMemory,
                "--driver-memory", driverMemory,
                "--total-executor-cores", totalExecutorCores,
                "--class", jarClass,
                "--jars", "/home/ubuntu/.m2/repository/com/github/scopt/scopt_2.11/3.7.0/scopt_2.11-3.7.0.jar " + jar,
                "--filePath", filePath,
                "----outfilePath", outFilePath,
        };

        SparkSubmit.main(SubmitString);
        return null;
    }


}
