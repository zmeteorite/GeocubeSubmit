package whu.edu.cn.dao;

import lombok.Data;

@Data
public class VectorDao {
    private String ProductKey;
    private String ProductName;
    private String CRS;
    private String PhenomenonTime;

    private String UpperLeftLat;
    private String UpperLeftLong;
    private String UpperRightLat;
    private String UpperRightLong;
    private String LowerLeftLat;
    private String LowerLeftLong;
    private String LowerRightLat;
    private String LowerRightLong;

    private String Properties;

}
