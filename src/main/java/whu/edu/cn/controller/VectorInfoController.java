package whu.edu.cn.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.JsonObject;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import whu.edu.cn.service.VectorInfoService;

import java.util.List;

@CrossOrigin(origins = "*",maxAge = 3600)
@RestController
@Api(tags = "矢量查询")
@RequestMapping("/Geocube")
public class VectorInfoController {
    @Autowired
    VectorInfoService vectorInfoService;

    @GetMapping("/GetVectorsByParams")
    @ApiOperation(value = "通过时空条件查询矢量",notes = "得到矢量的geojson")
    public List<JsonNode> GetVectorsJsons(@RequestParam(value = "productName",required = false) String productName,
                                          @RequestParam(value = "startTime",required = false) String StartTime,
                                          @RequestParam(value = "endTime",required = false) String EndTime,
                                          @RequestParam(value = "productType",required = true) String ProductType,
                                          @RequestParam(value = "minx",required = false,defaultValue = "-180.0")double minx,
                                          @RequestParam(value = "miny",required = false,defaultValue = "-90.0")double miny,
                                          @RequestParam(value = "maxx",required = false,defaultValue = "180.0")double maxx,
                                          @RequestParam(value = "maxy",required = false,defaultValue = "90.0")double maxy){
        if(ProductType.equals("Vector")){
            return vectorInfoService.queryVectorJsons(productName,minx,miny,maxx,maxy,StartTime,EndTime);
        } else{
            throw new RuntimeException("Not Vector Data!");
        }

    }

}
