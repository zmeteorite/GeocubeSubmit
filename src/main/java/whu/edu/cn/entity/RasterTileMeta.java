package whu.edu.cn.entity;

import whu.edu.cn.entity.Measurement;


public class RasterTileMeta {
    private String ID;
    private String ProductID;
    private String RowNum;
    private String ColNum;
    private String leftBottomLat;
    private String leftBottomLong;
    private String rightUpperLat;
    private String rightUpperLong;
    private Product ProductMeta;
    private Extent ExtentMeta;
    private TileQuality TileQualityMeta;
    private String CRS;
    private Measurement Measurement;

    public RasterTileMeta(){}
    public RasterTileMeta(String ID, String ProductID){
        this.ID =ID;
        this.ProductID = ProductID;
    }

    public void setProductMeta(Product productMeta) {
        ProductMeta = productMeta;
    }

    public void setMeasurement(Measurement measurement) {
        Measurement = measurement;
    }

    public void setExtentMeta(Extent extentMeta) {
        ExtentMeta = extentMeta;
    }

    public void setColNum(String colNum) {
        ColNum = colNum;
    }

    public void setRowNum(String rowNum) {
        RowNum = rowNum;
    }

    public void setCRS(String CRS) {
        this.CRS = CRS;
    }

    public void setLeftBottomLat(String leftBottomLat) {
        this.leftBottomLat = leftBottomLat;
    }

    public void setLeftBottomLong(String leftBottomLong) {
        this.leftBottomLong = leftBottomLong;
    }

    public void setRightUpperLat(String rightUpperLat) {
        this.rightUpperLat = rightUpperLat;
    }

    public void setRightUpperLong(String rightUpperLong) {
        this.rightUpperLong = rightUpperLong;
    }

    public void setTileQualityMeta(TileQuality tileQualityMeta) {
        TileQualityMeta = tileQualityMeta;
    }

    public String getColNum() {
        return ColNum;
    }

    public String getLeftBottomLat() {
        return leftBottomLat;
    }

    public String getLeftBottomLong() {
        return leftBottomLong;
    }

    public Product getProductMeta() {
        return ProductMeta;
    }

    public String getRightUpperLat() {
        return rightUpperLat;
    }

    public String getRightUpperLong() {
        return rightUpperLong;
    }

    public Extent getExtentMeta() {
        return ExtentMeta;
    }

    public String getRowNum() {
        return RowNum;
    }

    public String getCRS() {
        return CRS;
    }

    public whu.edu.cn.entity.Measurement getMeasurement() {
        return Measurement;
    }
}
