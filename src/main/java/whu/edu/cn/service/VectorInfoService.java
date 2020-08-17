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
        queryParams.setExtent(minx,miny,maxx,maxy);
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

//    public List<String> queryVectorJsonStrings(String vectorProductName,Double minx,Double miny,Double maxx,Double maxy,String startTime,String endTime){
//        QueryParams queryParams = new QueryParams();
//        queryParams.setVectorProductName(vectorProductName);
//        queryParams.setExtent(minx,miny,maxx,maxy);
//        queryParams.setTime(startTime,endTime);
//
//        List<String> jsons = QueryVectorObjects.getVectorGeoJsons2(queryParams);
////        List<JSONObject> jsonObjects = new ArrayList<JSONObject>();
//        List<JsonNode> jsonNodes = new ArrayList<JsonNode>();
//        ObjectMapper mapper = new ObjectMapper();
//        for (String str:jsons) {
//            try{
//                JsonNode root = mapper.readTree(str);
//                System.out.println(root);
//                jsonNodes.add(root);
//            } catch (IOException e){
//
//            }
//
//        }
//        return  jsons;
//    }
}
