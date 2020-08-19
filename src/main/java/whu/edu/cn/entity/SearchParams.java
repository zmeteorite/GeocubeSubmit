package whu.edu.cn.entity;

import java.util.ArrayList;
import java.util.List;


public class SearchParams {
    private String ProductName;
    private String Platform;
    private List<String> Instruments;
    private List<String> Measurements;
    private String CRS;
    private String Tilesize;
    private String Cellres;
    private String Level;
    private String PhenomenonTime;
    private String Starttime;
    private String Endtime;
    private List<String> GridCodes;
    private List<String> CityCodes;
    private List<String> CityNames;
    private String ProvinceName;
    private String DistrictName;
    private String CloudMax;
    private String CloudShadowMax;
    public SearchParams(){}
    public SearchParams(String Platform,List<String> Measurements,List<String> GridCodes,String Cloud,String Level){
        this.Platform=Platform;
        this.Measurements= Measurements;
        this.GridCodes=GridCodes;
        this.CloudMax=Cloud;
        this.Level=Level;
        this.ProductName="";
        this.Instruments=new ArrayList<>();

        this.CRS="";
        this.Tilesize="";
        this.Cellres="";
        this.Starttime="";
        this.Endtime= "";
        this.PhenomenonTime="";

        this.CityCodes=new ArrayList<>();
        this.CityNames=new ArrayList<>();

        this.CloudShadowMax="";

    }
    public SearchParams(String ProductName,String Platform,List<String> Instruments,
                        List<String> Measurements,String CRS,String Tilesize,String Cellres,
                        String Starttime,String Endtime,String Phenomenontime,List<String> GridCodes,
                        List<String> CityCodes,List<String> CityNames,String Cloud,String CloudShadow,String Level){
        this.ProductName=ProductName;
        this.Platform=Platform;
        this.Instruments=Instruments;
        this.Measurements=Measurements;
        this.CRS=CRS;
        this.Tilesize=Tilesize;
        this.Cellres=Cellres;
        this.Starttime=Starttime;
        this.Endtime= Endtime;
        this.PhenomenonTime=Phenomenontime;
        this.GridCodes=GridCodes;
        this.CityCodes=CityCodes;
        this.CityNames=CityNames;
        this.CloudMax=Cloud;
        this.CloudShadowMax=CloudShadow;
        this.Level=Level;
    }

    public void setProductNameParam(String productName) {
        ProductName = productName;
    }

    public void setPlatformParam(String platform) {
        Platform = platform;
    }

    public void setMeasurementParams(List<String> measurements) {
        Measurements = measurements;
    }

    public void setLevelParam(String level) {
        Level = level;
    }

    public void setStarttimeParam(String starttime) {
        Starttime = starttime;
    }

    public void setEndtimeParam(String endtime) {
        Endtime = endtime;
    }

    public void setGridCodeParams(List<String> gridCodes) {
        GridCodes = gridCodes;
    }

    public void setCellresParam(String cellres) {
        Cellres = cellres;
    }

    public void setCityCodeParams(List<String> cityCodes) {
        CityCodes = cityCodes;
    }

    public void setCityNameParams(List<String> cityNames) {
        CityNames = cityNames;
    }

    public void setCRSParam(String CRS) {
        this.CRS = CRS;
    }

    public void setCloudMaxParam(String cloudMax) {
        CloudMax = cloudMax;
    }

    public void setCloudShadowMaxParam(String cloudShadowMax) {
        CloudShadowMax = cloudShadowMax;
    }

    public void setInstrumentParams(List<String> instruments) {
        Instruments = instruments;
    }

    public void setTilesizeParam(String tilesize) {
        Tilesize = tilesize;
    }

    public void setDistrictNameParam(String districtName) {
        DistrictName = districtName;
    }

    public String getPlatform() {
        return Platform;
    }

    public String getPhenomenonTime() {
        return PhenomenonTime;
    }

    public String getProductName() {
        return ProductName;
    }

    public String getStarttime() {
        return Starttime;
    }

    public String getEndtime() {
        return Endtime;
    }

    public String getLevel() {
        return Level;
    }

    public void setTilesize(String tilesize) {
        Tilesize = tilesize;
    }

    public String getTilesize() {
        return Tilesize;
    }

    public void setMeasurements(List<String> measurements) {
        Measurements = measurements;
    }

    public List<String> getGridCodes() {
        return GridCodes;
    }

    public String getCloudMax() {
        return CloudMax;
    }

    public String getCloudShadowMax() {
        return CloudShadowMax;
    }

    public String getCRS() {
        return CRS;
    }

    public String getCellres() {
        return Cellres;
    }

    public List<String> getMeasurements() {
        return Measurements;
    }

    public List<String> getCityNames() {
        return CityNames;
    }

    public List<String> getCityCodes() {
        return CityCodes;
    }

    public List<String> getInstruments() {
        return Instruments;
    }

    public String getDistrictName() {
        return DistrictName;
    }

    public String getProvinceName() {
        return ProvinceName;
    }
}
