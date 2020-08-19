package whu.edu.cn.dao;

import lombok.Data;

@Data
public class ProductDao {
    private String ProductKey;
    private String ProductName;
    private String PlatformName;
    private String SensorName;
    private String MeasurementName;
    private String DType;
    private String CRS;
//    private String Tilesize;
//    private String Cellres;
//    private String Level;

    private String ResultTime;
    private String PhenomenonTime;
    private String ImagingLength;
    private String ImagingWidth;

    private String UpperLeftLat;
    private String UpperLeftLong;
    private String UpperRightLat;
    private String UpperRightLong;
    private String LowerLeftLat;
    private String LowerLeftLong;
    private String LowerRightLat;
    private String LowerRightLong;

    public ProductDao(){}
}
