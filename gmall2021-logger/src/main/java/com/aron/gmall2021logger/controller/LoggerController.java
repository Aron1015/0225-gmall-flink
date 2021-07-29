package com.aron.gmall2021logger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


//@RestController=@Controller+@ResponseBody
@RestController//表示返回的是普通对象而不是页面
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    @RequestMapping("test1")
    public String test1(){
        System.out.println("11111111111");
        return "success";
    }

    @RequestMapping("test2")
    public String test2(@RequestParam("name") String name, @RequestParam(value = "age",defaultValue = "18")  int age) {
        System.out.println(name+":"+age);
        return name+":  "+age;
    }

    @RequestMapping("applog")
    public String getLogger(@RequestParam("param") String jsonStr) {
        //将日志数据打印到控制台&写入日志文件
        log.info(jsonStr);

        //将数据写入kafka
        kafkaTemplate.send("ods_base_log",jsonStr);

//        System.out.println(jsonStr);
        return "success";

    }
}
