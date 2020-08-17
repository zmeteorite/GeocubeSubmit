package whu.edu.cn.query.entity

import geotrellis.layer.LayoutDefinition
import geotrellis.raster.TileLayout
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import whu.edu.cn.query.entity.Extent._

import scala.collection.mutable.ArrayBuffer

class QueryParams {
  var rasterProductName: String = ""
  var rasterProductNames: ArrayBuffer[String] = new ArrayBuffer[String]()
  var vectorProductName: String = ""
  var vectorProductNames: ArrayBuffer[String] = new ArrayBuffer[String]()
  var platform: String = ""
  var instruments: ArrayBuffer[String] = new ArrayBuffer[String]()
  var measurements: ArrayBuffer[String] = new ArrayBuffer[String]()
  var CRS: String = ""
  var tileSize: String = ""
  var cellRes: String = ""
  var level: String = "999"
  var phenomenonTime: String = ""
  var startTime: String = ""
  var endTime: String = ""
  var nextStartTime: String = ""
  var nextEndTime: String = ""
  var gridCodes: ArrayBuffer[String] = new ArrayBuffer[String]()
  var cityCodes: ArrayBuffer[String] = new ArrayBuffer[String]()
  var cityNames: ArrayBuffer[String] = new ArrayBuffer[String]()
  var provinceName: String = ""
  var districtName: String = ""
  var cloudMax: String = ""
  var cloudShadowMax: String = ""


  def this(platform: String, measurements: ArrayBuffer[String],
           gridCodes: ArrayBuffer[String], cloud: String, level: String){
    this()
    this.platform = platform
    this.measurements = measurements
    this.gridCodes = gridCodes
    this.cloudMax = cloud
    this.level = level
  }

  def this(rasterProductName: String, platform: String, instruments: ArrayBuffer[String],
           measurements: ArrayBuffer[String], CRS: String, tileSize: String,
           cellRes: String, level: String, phenomenonTime: String,
           startTime: String, endTime: String, gridCodes: ArrayBuffer[String],
           cityCodes: ArrayBuffer[String], cityNames: ArrayBuffer[String], cloud: String, cloudShadow: String){
    this()
    this.rasterProductName = rasterProductName
    this.platform = platform
    this.instruments = instruments
    this.measurements = measurements
    this.CRS = CRS
    this.tileSize = tileSize
    this.cellRes = cellRes
    this.startTime = startTime
    this.endTime = endTime
    this.nextStartTime = nextStartTime
    this.nextEndTime = nextEndTime
    this.phenomenonTime = phenomenonTime
    this.gridCodes = gridCodes
    this.cityCodes = cityCodes
    this.cityNames = cityNames
    this.cloudMax = cloud
    this.cloudShadowMax = cloudShadow
    this.level = level
  }

  def this(rasterProductNames: ArrayBuffer[String], platform: String, instruments: ArrayBuffer[String],
           measurements: ArrayBuffer[String], CRS: String, tileSize: String,
           cellRes: String, level: String, phenomenonTime: String,
           startTime: String, endTime: String, gridCodes: ArrayBuffer[String],
           cityCodes: ArrayBuffer[String], cityNames: ArrayBuffer[String], cloud: String, cloudShadow: String){
    this()
    this.rasterProductNames = rasterProductNames
    this.platform = platform
    this.instruments = instruments
    this.measurements = measurements
    this.CRS = CRS
    this.tileSize = tileSize
    this.cellRes = cellRes
    this.startTime = startTime
    this.endTime = endTime
    this.nextStartTime = nextStartTime
    this.nextEndTime = nextEndTime
    this.phenomenonTime = phenomenonTime
    this.gridCodes = gridCodes
    this.cityCodes = cityCodes
    this.cityNames = cityNames
    this.cloudMax = cloud
    this.cloudShadowMax = cloudShadow
    this.level = level
  }

  def setExtent(leftBottomLong:Double, LeftBottomLat:Double, rightUpperLong:Double, rightUpperLat:Double): Unit = {
    val queryGeometry = new GeometryFactory().createPolygon(Array(
      new Coordinate(leftBottomLong, LeftBottomLat),
      new Coordinate(leftBottomLong, rightUpperLat),
      new Coordinate(rightUpperLong, rightUpperLat),
      new Coordinate(rightUpperLong, LeftBottomLat),
      new Coordinate(leftBottomLong, LeftBottomLat)
    ))
    val extent = geotrellis.vector.Extent(-180, -90, 180, 90)
    val tl = TileLayout(360, 180, 1024, 1024)
    val ld = LayoutDefinition(extent, tl)
    /*val gridCodeArray = this.level match {
      case "" => geom2GridCode(queryGeometry, ld.layoutCols, ld.layoutRows, ld.extent)
      case _ => geom2GridCode(queryGeometry, ld.layoutCols, ld.layoutRows, ld.extent, this.level.toInt)
    }*/
    val gridCodeArray = geom2GridCode(queryGeometry, ld.layoutCols, ld.layoutRows, ld.extent)
    /*print("GridCodes: ")
    gridCodeArray.foreach(x=>print(x + " "))*/
    this.setGridCodes(gridCodeArray)
  }

  def setTime(startTime:String, endTime:String): Unit = {
    this.setStartTime(startTime)
    this.setEndTime(endTime)
  }

  def setPreviousTime(previousStartTime:String, previousEndTime:String): Unit = {
    this.setTime(previousStartTime, previousEndTime)
  }

  def setNextTime(nextStartTime:String, nextEndTime:String): Unit = {
    this.setNextStartTime(nextStartTime)
    this.setNextEndTime(nextEndTime)
  }

  def setRasterProductName(rasterProductName: String): Unit = {
    this.rasterProductName = rasterProductName
  }

  def setRasterProductNames(rasterProductNames: ArrayBuffer[String]): Unit = {
    this.rasterProductNames = rasterProductNames
  }

  def setRasterProductNames(rasterProductNames: Array[String]): Unit = {
    this.rasterProductNames = rasterProductNames.toBuffer.asInstanceOf[ArrayBuffer[String]]
  }
  def setVectorProductName(vectorProductName: String): Unit = {
    this.vectorProductName = vectorProductName
  }

  def setVectorProductNames(vectorProductNames: ArrayBuffer[String]): Unit = {
    this.vectorProductNames = vectorProductNames
  }

  def setVectorProductNames(vectorProductNames: Array[String]): Unit = {
    this.vectorProductNames = vectorProductNames.toBuffer.asInstanceOf[ArrayBuffer[String]]
  }
  def setPlatform(platform: String): Unit = {
    this.platform = platform
  }

  def setInstruments(instruments: ArrayBuffer[String]): Unit = {
    this.instruments = instruments
  }

  def setMeasurements(measurements: Array[String]): Unit = {
    this.measurements = measurements.toBuffer.asInstanceOf[ArrayBuffer[String]]
  }

  def setMeasurements(measurements: ArrayBuffer[String]): Unit = {
    this.measurements = measurements
  }

  def setLevel(level: String): Unit = {
    this.level = level
  }

  def setCRS(CRS: String): Unit = {
    this.CRS = CRS
  }

  def setCloudMax(cloudMax: String): Unit = {
    this.cloudMax = cloudMax
  }

  def setCloudShadowMax(cloudShadowMax: String): Unit = {
    this.cloudShadowMax = cloudShadowMax
  }

  /*---------------*/
  def setStartTime(startTime: String): Unit = {
    this.startTime = startTime
  }

  def setEndTime(endTime: String): Unit = {
    this.endTime = endTime
  }

  def setNextStartTime(nextStartTime: String): Unit = {
    this.nextStartTime = nextStartTime
  }

  def setNextEndTime(nextEndTime: String): Unit = {
    this.nextEndTime = nextEndTime
  }

  def setGridCodes(gridCodes: Array[String]): Unit = {
    this.gridCodes = gridCodes.toBuffer.asInstanceOf[ArrayBuffer[String]]
  }

  def setGridCodes(gridCodes: ArrayBuffer[String]): Unit = {
    this.gridCodes = gridCodes
  }

  def setCellRes(cellRes: String): Unit = {
    this.cellRes = cellRes
  }

  def setCityCodes(cityCodes: ArrayBuffer[String]): Unit = {
    this.cityCodes = cityCodes
  }

  def setCityNames(cityNames: ArrayBuffer[String]): Unit = {
    this.cityNames = cityNames
  }

  def setDistrictName(districtName: String): Unit = {
    this.districtName = districtName
  }

  def getPlatform: String = platform

  def getPhenomenonTime: String = phenomenonTime

  def getRasterProductName: String = rasterProductName

  def getRasterProductNames: ArrayBuffer[String] = rasterProductNames

  def getVectorProductName: String = vectorProductName

  def getVectorProductNames: ArrayBuffer[String] = vectorProductNames

  def getStartTime: String = startTime

  def getEndTime: String = endTime

  def getNextStartTime: String = nextStartTime

  def getNextEndTime: String = nextEndTime

  def getLevel: String = level

  def setTileSize(tileSize: String): Unit = {
    this.tileSize = tileSize
  }

  def getTileSize: String = tileSize

  def getGridCodes: ArrayBuffer[String] = gridCodes

  def getCloudMax: String = cloudMax

  def getCloudShadowMax: String = cloudShadowMax

  def getCRS: String = CRS

  def getCellRes: String = cellRes

  def getMeasurements: ArrayBuffer[String] = measurements

  def getCityNames: ArrayBuffer[String] = cityNames

  def getCityCodes: ArrayBuffer[String] = cityCodes

  def getInstruments: ArrayBuffer[String] = instruments

  def getDistrictName: String = districtName

  def getProvinceName: String = provinceName

}
