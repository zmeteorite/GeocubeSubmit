package whu.edu.cn.entity;

import lombok.Data;

import java.util.List;

@Data
public class Product {
    private String ProductKey;
    private String ProductName;
    private String PlatformName;
    private String SensorName;
    private List<Measurement> Measurements;
    private String CRS;
    private String Tilesize;
    private String Cellres;
    private String Level;
    private String PhenomenonTime;
    private String ImagingLength;
    private String ImagingWidth;
    private String ResultTime;
    private String UpperLeftLat;
    private String UpperLeftLong;
    private String UpperRightLat;
    private String UpperRightLong;
    private String LowerLeftLat;
    private String LowerLeftLong;
    private String LowerRightLat;
    private String LowerRightLong;

    public Product(){}

    public void setMeasurements(List<Measurement> measurements) {
        this.Measurements = measurements;
    }

}
