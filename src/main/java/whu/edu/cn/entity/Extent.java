package whu.edu.cn.entity;

import lombok.Data;

@Data
public class Extent {
    String ExtentKey;
    String GridCode;
    String CityCode;
    String CityName;
    String ProvinceName;
    String DistrictName;
    String Extent;

    public Extent(){}
}
