package whu.edu.cn.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonObject;
import org.json.simple.JSONObject;

import org.json4s.jackson.Json;
import org.springframework.stereotype.Service;
import whu.edu.cn.query.entity.QueryParams;
import whu.edu.cn.query.service.QueryVectorObjects;


import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

@Service
public class VectorInfoService {
    public List<JsonNode> queryVectorJsons(String vectorProductName,Double minx,Double miny,Double maxx,Double maxy,String startTime,String endTime){
        QueryParams queryParams = new QueryParams();
        queryParams.setVectorProductName(vectorProductName);
        if(minx==-180.0&&miny==-90.0&&maxx==180.0&&maxy==90.0){
            System.out.println("不设置空间范围");
        } else {
            queryParams.setExtent(minx,miny,maxx,maxy);
        }
        queryParams.setTime(startTime,endTime);

        List<String> jsons = QueryVectorObjects.getVectorGeoJsons2(queryParams);
        List<JsonNode> jsonNodes = new ArrayList<JsonNode>();
        ObjectMapper mapper = new ObjectMapper();
        for (String str:jsons) {
            try{
                JsonNode root = mapper.readTree(str);
//                System.out.println(root);
                jsonNodes.add(root);
            } catch (IOException e){

            }

        }
        return  jsonNodes;
    }

    public String queryVectorProperty(String vectorProductName){
        QueryParams queryParams = new QueryParams();
        queryParams.setVectorProductName(vectorProductName);
//        queryParams.setExtent(minx,miny,maxx,maxy);
//        queryParams.setTime(startTime,endTime);

        String properties = QueryVectorObjects.getVectorGeoJsons3(queryParams);

        return  properties;
    }
}
