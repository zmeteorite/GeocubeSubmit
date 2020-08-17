package whu.edu.cn.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.JsonObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import whu.edu.cn.service.VectorInfoService;

import java.util.List;

@RestController
@RequestMapping("/Geocube")
public class VectorInfoController {
    @Autowired
    VectorInfoService vectorInfoService;

    @GetMapping("/GetVectorsByParams")
    public List<JsonNode> GetVectorsJsons(@RequestParam(value = "productName",required = false) String productName,
                                          @RequestParam(value = "startTime",required = false) String StartTime,
                                          @RequestParam(value = "endTime",required = false) String EndTime,
                                          @RequestParam(value = "productType",required = true) String ProductType,
                                          @RequestParam("minx")double minx,
                                          @RequestParam("miny")double miny,
                                          @RequestParam("maxx")double maxx,
                                          @RequestParam("maxy")double maxy){
        if(ProductType.equals("Vector")){
            return vectorInfoService.queryVectorJsons(productName,minx,miny,maxx,maxy,StartTime,EndTime);
        } else{
            throw new RuntimeException("Not Vector Data!");
        }

    }

}
