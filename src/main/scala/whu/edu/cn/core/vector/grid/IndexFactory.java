package whu.edu.cn.core.vector.grid;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 希尔伯特曲线编码索引工厂类
 */
public class IndexFactory implements Serializable {

    private double minX;
    private double maxX;
    private double minY;
    private double maxY;
    private int minLevel;
    private int maxLevel;
    private int maxNum;

    public double getMinX() {
        return minX;
    }

    public double getMaxX() {
        return maxX;
    }

    public double getMinY() {
        return minY;
    }

    public double getMaxY() {
        return maxY;
    }

    public void setMaxY(double maxY) {
        this.maxY = maxY;
    }

    public int getMinLevel() {
        return minLevel;
    }

    public void setMinLevel(int minLevel) {
        this.minLevel = minLevel;
    }

    public int getMaxLevel() {
        return maxLevel;
    }

    public void setMaxLevel(int maxLevel) {
        this.maxLevel = maxLevel;
    }

    public int getMaxNum() {
        return maxNum;
    }

    public void setMaxNum(int maxNum) {
        this.maxNum = maxNum;
    }

    /**
     * 初始化方法一
     *
     * @param minX     最小X
     * @param maxX     最大X
     * @param minY     最小Y
     * @param maxY     最大Y
     * @param minLevel 最小网格层级（0-30）
     * @param maxLevel 最大网格层级（0-30）
     * @param maxNum   最大数量
     */
    public IndexFactory(double minX, double maxX, double minY, double maxY, int minLevel, int maxLevel, int maxNum) {
        Transformer.init(minX, maxX, minY, maxY);
        this.minX = minX;
        this.maxX = maxX;
        this.minY = minY;
        this.maxY = maxY;
        this.minLevel = minLevel;
        this.maxLevel = maxLevel;
        this.maxNum = maxNum;
    }

    /**
     * 初始化方法二
     *
     * @param minX    最小X
     * @param maxX    最大X
     * @param minY    最小Y
     * @param maxY    最大Y
     * @param minSize 最小网格尺寸（单位与XY相同）
     * @param maxSize 最大网格尺寸（单位与XY相同）
     * @param maxNum  最大数量
     */
    public IndexFactory(double minX, double maxX, double minY, double maxY, double minSize, double maxSize, int maxNum) {
        Transformer.init(minX, maxX, minY, maxY);
        this.minX = minX;
        this.maxX = maxX;
        this.minY = minY;
        this.maxY = maxY;
        this.minLevel = Transformer.getLevel(maxSize, maxSize);
        this.maxLevel = Transformer.getLevel(minSize, minSize);
        this.maxNum = maxNum;
    }

    /**
     * 创建索引
     *
     * @param x X坐标
     * @param y Y坐标
     * @return 索引字符串
     */
    public String createIndex(double x, double y) {
        Point point = new GeometryFactory().createPoint(new Coordinate(x, y));
        return this.createIndex(point);
    }

    /**
     * 创建坐标
     *
     * @param point 空间点对象
     * @return 索引字符串
     */
    public String createIndex(Point point) {
        return CellId.fromPoint(point).parent(this.maxLevel).toToken();
    }

    /**
     * 生成网格索引列表
     *
     * @param strWKT 几何对象的WKT字符串
     * @return 索引列表
     */
    public List<String> createIndexList(String strWKT) throws ParseException {
        WKTReader wktReader = new WKTReader();
        Geometry region = wktReader.read(strWKT);
        return this.createIndexList(region);
    }

    /**
     * 生成网格索引列表
     *
     * @param region 几何对象
     * @return 索引列表
     */
    public List<String> createIndexList(Geometry region) {
        if (this.minLevel > this.maxLevel) {
            return new ArrayList<>();
        } else if (this.minLevel == this.maxLevel) {
            return this.createSimpleCovering(region);
        } else {
            return this.createComplexCovering(region);
        }
    }

    private List<String> createSimpleCovering(Geometry region) {
        Point start = region.getInteriorPoint();
        ArrayList<CellId> cellIds = new ArrayList<>();
        RegionCoverer.getSimpleCovering(region, start, this.minLevel, cellIds);
        return cellIds.stream().map(cellId -> cellId.toToken()).collect(Collectors.toList());
    }

    private List<String> createComplexCovering(Geometry region) {
        RegionCoverer regionCoverer = new RegionCoverer();
        regionCoverer.setMinLevel(this.minLevel);
        regionCoverer.setMaxLevel(this.maxLevel);
        regionCoverer.setMaxCells(this.maxNum);
        ArrayList<CellId> cellIds = new ArrayList<>();
        regionCoverer.getCovering(region, cellIds);
        return cellIds.stream().map(cellId -> cellId.toToken()).collect(Collectors.toList());
    }
}

