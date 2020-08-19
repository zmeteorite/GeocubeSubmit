package whu.edu.cn.util;

import org.springframework.stereotype.Component;

import java.util.HashMap;

/**
 * @author czp
 * @version 1.0
 * @date 2019/10/12 16:50
 */
@Component
public class RsqGsUtil {
    public String DoubleToWKT(double minx,double miny,double maxx,double maxy){
        String WKT_rec="POLYGON(("+minx+" "+miny+", "+minx+" "+maxy+", "+maxx+" "+maxy+", "+maxx+" "+miny+","+minx+" "+miny+"))";
        return WKT_rec;
    }
    public String getGeom(HashMap<String,String> informations){
        String WKT_geom=null;
        if (informations.get("TopLeftLatitude")!=null && informations.get("TopLeftLongitude")!=null
                && informations.get("TopRightLatitude")!=null && informations.get("TopRightLongitude")!=null
                && informations.get("BottomRightLatitude")!=null && informations.get("BottomRightLongitude")!=null
                && informations.get("BottomLeftLatitude")!=null && informations.get("BottomLeftLongitude")!=null){
            double TopLeftLatitude=Double.parseDouble(informations.get("TopLeftLatitude"));
            double TopLeftLongitude=Double.parseDouble(informations.get("TopLeftLongitude"));
            double TopRightLatitude=Double.parseDouble(informations.get("TopRightLatitude"));
            double TopRightLongitude=Double.parseDouble(informations.get("TopRightLongitude"));
            double BottomRightLatitude=Double.parseDouble(informations.get("BottomRightLatitude"));
            double BottomRightLongitude=Double.parseDouble(informations.get("BottomRightLongitude"));
            double BottomLeftLatitude=Double.parseDouble(informations.get("BottomLeftLatitude"));
            double BottomLeftLongitude=Double.parseDouble(informations.get("BottomLeftLongitude"));
            WKT_geom="POLYGON(("+TopLeftLongitude+" "+TopLeftLatitude+", "+TopRightLongitude+" "+TopRightLatitude+", "+BottomRightLongitude+" "+BottomRightLatitude
                    +", "+BottomLeftLongitude+" "+BottomLeftLatitude+", "+TopLeftLongitude+" "+TopLeftLatitude+"))";
        }
        return WKT_geom;
    }
}
