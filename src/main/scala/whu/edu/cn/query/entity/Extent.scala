package whu.edu.cn.query.entity

import java.sql.{DriverManager, ResultSet}

import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster.TileLayout
import geotrellis.store.index.{KeyIndex, ZCurveKeyIndexMethod}
import org.locationtech.jts.geom.Geometry

import scala.collection.mutable.ArrayBuffer

/**
  * Tile的空间范围属性 包括ID，格网ID，城市区域ID等，以及空间大小，分辨率和层级
  * @param _extentID
  * @param _gridCode
  * @param _cityCode
  * @param _cityName
  * @param _provinceName
  * @param _districtName
  * @param _extent
  * @param _tilesize
  * @param _cellres
  * @param _level
  */
case class Extent(_extentID: String = "", _gridCode: String = "",
                  _cityCode: String = "", _cityName: String = "",
                  _provinceName: String = "",_districtName: String = "",
                  _extent: String = "", _tilesize: String = "", _cellres: String = "", _level: String = ""
                 ) {
  var extentID: String = _extentID
  var gridCode: String = _gridCode
  var cityCode: String = _cityCode
  var cityName: String = _cityName
  var provinceName: String = _provinceName
  var districtName: String = _districtName
  var extent: String = _extent
  var tilesize: String = _tilesize
  var cellres: String = _cellres
  var level: String = _level

  def setExtent(_extent: String): Unit = {
    extent = _extent
  }

  def getExtent: String = extent

  def setTileSize(_tilesize: String): Unit = {
    tilesize = _tilesize
  }
  def getTileSize:String = tilesize

  def setCellRes(_cellres: String): Unit = {
    cellres = _cellres
  }
  def getCellRes:String = cellres

  def setLevel(_level: String): Unit = {
    level = _level
  }
  def getLevel:String = level
}

object Extent{
  /**
    * 从数据库查询Tile的空间属性
    * @param extentKey
    * @param conn
    * @return
    */
  def getExtentMetaByKey(extentKey: String, conn:java.sql.Connection): whu.edu.cn.query.entity.Extent = {
//    val conn = DriverManager.getConnection(connAddr, user, password)
    if (conn != null) {
      try {

        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        // Execute Query
        val sql = "Select extent_key,grid_code,city_code,city_name,province_name,district_name,extent " +
          "from gc_extent where extent_key = " + extentKey + ";"
        val rs = statement.executeQuery(sql)
        val rsArray = new Array[String](7);
        val columnCount = rs.getMetaData().getColumnCount()
        //each tile has unique extent object
        while (rs.next) {
          for (i <- 1 to columnCount)
            rsArray(i - 1) = rs.getString(i)
        }
        //println("Extent meta of the queried extentKey:")
        /*println("extentID/Key:" + rsArray(0),
          "gridCode:" + rsArray(1),
          "cityCode:" + rsArray(2),
          "cityName:" + rsArray(3),
          "provinceName:" + rsArray(4),
          "districtName:" + rsArray(5),
          "gridExtent:" + rsArray(6))*/

        val extent = new whu.edu.cn.query.entity.Extent(rsArray(0), rsArray(1), rsArray(2), rsArray(3), rsArray(4), rsArray(5), rsArray(6))
        extent
      } finally {
        conn.close
      }
    } else
      throw new RuntimeException("Null connection!")
  }

  /**
    * 实现Geotrellis的几何对象到格网数组的转换，便于利用空间坐标进行查询
    * @param x
    * @param gridDimX
    * @param gridDimY
    * @param extent
    * @return
    */
  def geom2GridCode(x: Geometry, gridDimX: Long, gridDimY: Long, extent: geotrellis.vector.Extent): ArrayBuffer[String] = {
    val spatialKeyResults: ArrayBuffer[SpatialKey] = new ArrayBuffer[SpatialKey]()
    val gridcodeResults: ArrayBuffer[String] = new ArrayBuffer[String]()
    val geom = x
    val mbr = (geom.getEnvelopeInternal.getMinX,geom.getEnvelopeInternal.getMaxX,
      geom.getEnvelopeInternal.getMinY, geom.getEnvelopeInternal.getMaxY)

    val gridSizeX = (extent.xmax - extent.xmin) / gridDimX.toDouble
    val gridSizeY = (extent.ymax - extent.ymin) / gridDimY.toDouble
    val _xmin: Int = (math.floor((mbr._1 - extent.xmin) / gridSizeX) max 0).toInt
    val _xmax: Int = (math.ceil((mbr._2 - extent.xmin) / gridSizeX) min gridDimX).toInt
    val _ymin: Int = (math.floor((mbr._3 - extent.ymin) / gridSizeY) max 0).toInt
    val _ymax: Int = (math.ceil((mbr._4 - extent.ymin) / gridSizeY) min gridDimY).toInt


    val tl = TileLayout(360, 180, gridDimX.toInt, gridDimY.toInt) //tile:1°×1°, gridDimX and gridDimY are not used in tl and ld:

    val ld = LayoutDefinition(extent, tl)
    for (i <- _xmin to _xmax; j <- _ymin to _ymax) {
      val geotrellis_j = (gridDimY - 1 - j).toInt //geotrellis中网格xy编码为从左到右从上至下，而此处的y为从下至上，因此转换
      val gridExtent = ld.mapTransform.keyToExtent(i, geotrellis_j)
      if (gridExtent.intersects(geom)) {
        spatialKeyResults.append(SpatialKey(i, geotrellis_j))
        //spatialKeyResults.append(SpatialKey(i, 180 - geotrellis_j - 1))
      }
    }

    val keyIndex: KeyIndex[SpatialKey] = ZCurveKeyIndexMethod.createIndex(null)
    spatialKeyResults.foreach(sk=>gridcodeResults.append(keyIndex.toIndex(sk).toString()))

    /*for(i <- 0 until gridcodeResults.length)
      println("column, row , gridcode= " + spatialKeyResults(i).col + ", " +  spatialKeyResults(i).row + ", " + gridcodeResults(i))*/

    gridcodeResults
  }

  /**
   * Geotrellis：{column: 从左向右; row: 从上向下}, 当前函数为基于geotrellis
   * OsGeo：{column: 从左向右; row: 从下向上}, 将当前函数column改为column sum - column -1
   * */
  def xy2GridCode(column: Int, row: Int, level: Int): String = {
    val keyIndex: KeyIndex[SpatialKey] = ZCurveKeyIndexMethod.createIndex(null)
    val sk = SpatialKey(column, row)
    keyIndex.toIndex(sk).toString()
  }


}
