package whu.edu.cn.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import whu.edu.cn.config.spark.SparkAppParas;
import whu.edu.cn.service.SparkSubmitService;

import javax.servlet.http.HttpSession;
import java.io.File;
import java.io.IOException;
import java.util.UUID;

@RestController
public class SparkSubmitController {
    final String localDataRoot = "/home/geocube/tomcat8/apache-tomcat-8.5.57/webapps/data/temp/";
    final String httpDataRoot = "http://125.220.153.26:8093/data/temp/";


    @Autowired
    SparkSubmitService sparkSubmitService;
    @Autowired
    SparkAppParas sparkAppParas;

    @GetMapping("/submit/{func}")
    public String submit(@PathVariable("func") String func,
                         @RequestParam(value = "rasterProductName", required = true) String rasterProductName,
                         @RequestParam(value = "extent", required = true) String extent,
                         @RequestParam(value = "startTime", required = true) String startTime,
                         @RequestParam(value = "endTime", required = true) String endTime,
                         HttpSession session
                         /*@RequestParam(value = "measurements", defaultValue = "") String measurements*/)
            throws IOException, InterruptedException{
        session.setAttribute("state", "STARTED, 0%");

        String sessionId = session.getId();
        String uuid = UUID.randomUUID().toString();

        String localOutputDir = localDataRoot + sessionId + "/" + uuid + "/";
        File sessionFile=new File(localDataRoot + sessionId);
        if(!sessionFile.exists()) sessionFile.mkdir();
        File uuidFile=new File(localDataRoot + sessionId + "/" + uuid);
        if(!uuidFile.exists()) uuidFile.mkdir();

        sparkSubmitService.submitApp(sparkAppParas, func, rasterProductName, extent, startTime, endTime, localOutputDir, session);

        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode node = objectMapper.createObjectNode();
        File resultsDir = new File(localOutputDir);
        File[] subFiles = resultsDir.listFiles();
        int record = 0;
        for (File subFile : subFiles){
            if(subFile.getName().endsWith(".json")){
                ObjectMapper objMap = new ObjectMapper();
                JsonNode root = objMap.readTree(subFile);
                node.put(String.valueOf(record), root);
                record++;
            }
        }
        return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(node);
    }

    @GetMapping("/state")
    public String getState(HttpSession session){
        return (String) session.getAttribute("state");
    }

}
