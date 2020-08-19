package whu.edu.cn.entity;

import lombok.Data;

@Data
public class Measurement {
    private String MeasurementKey;
    private String MeasurementName;
    private String DType;

    public Measurement(){}

}
