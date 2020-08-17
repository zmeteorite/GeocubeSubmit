package whu.edu.cn.query.entity

import java.sql.{DriverManager, ResultSet}

import scala.collection.mutable.ArrayBuffer

case class Product (_productID: String = "", _productName: String = "", _platform: String = "",
                    _instrument: String = "", _CRS: String = "",_tilesize: String = "",
                    _cellres: String = "", _level: String = "", _phenomenonTime: String = "",
                    _height: String = "", _width: String = ""){
  var productID: String = _productID
  var productName: String = _productName
  var platform: String = _platform
  var instrument: String = _instrument
  var CRS: String = _CRS
  var tilesize: String = _tilesize
  var cellres: String = _cellres
  var level: String = _level
  var phenomenonTime: String = _phenomenonTime
  var height: String = _height
  var width: String = _width
  var measurements:ArrayBuffer[Measurement] = new ArrayBuffer[Measurement]()

  def getProductID: String = productID

  def getProductName: String = productName

  def setProductName(_productName: String): Unit = {
    this.productName = _productName
  }

  def getMeasurements: ArrayBuffer[Measurement] = measurements

  def setMeasurements(_measurements: ArrayBuffer[Measurement]): Unit = {
    this.measurements = _measurements
  }

  def getPlatform: String = platform

  def getPhenomenonTime: String = phenomenonTime

  def getInstrument: String = instrument
}

object Product{
  //return unique product
  def getProductMetaByKey(productKey: String, connAddr: String, user: String, password: String): Product = {
    val conn = DriverManager.getConnection(connAddr, user, password)
    if (conn != null) {
      try {
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        val sql = "select product_key,product_name,platform_name,sensor_name,tile_size,cell_res,crs,level,phenomenon_time,imaging_length,imaging_width " +
          "from \"SensorLevelAndProduct\" where product_key=" + productKey + ";"
        val rs = statement.executeQuery(sql)
        val rsArray = new Array[String](11);
        val columnCount = rs.getMetaData().getColumnCount();

        while (rs.next) {
          for (i <- 1 to columnCount)
            rsArray(i - 1) = rs.getString(i)
        }

        val productID = rsArray(0)
        val productName = rsArray(1)
        val productPlatform = rsArray(2)
        val productInstrument = rsArray(3)
        val productCRS = rsArray(6)
        val productTileSize = rsArray(4)
        val productRes = rsArray(5)
        val productLevel = rsArray(7)
        val productPhenomenonTime = rsArray(8)
        val productHeight = rsArray(9)
        val productWidth = rsArray(10)
        //println("Product meta of the queried product key:")
        /*println("productID/Key = " + productID,
          "productName = " + productName,
          "productPlatform = " + productPlatform,
          "productInstrument = " + productInstrument,
          "productCRS = " + productCRS,
          "productTileSize = " + productTileSize,
          "productRes = " + productRes,
          "productLevel = " + productLevel,
          "productPhenomenonTime = " + productPhenomenonTime,
          "productHeight = " + productHeight,
          "productWidth = " + productWidth)*/

        val product = Product(productID, productName, productPlatform, productInstrument, productCRS,
          productTileSize, productRes, productLevel, productPhenomenonTime, productHeight, productWidth)

        val sql2 = "select measurement_key, measurement_name, dtype " +
          "from \"MeasurementsAndProduct\" where product_key=" + productKey + ";"

        val rs2 = statement.executeQuery(sql2)
        val columnCount2 = rs2.getMetaData().getColumnCount()
        val measurementlist = new ArrayBuffer[Measurement]()
        while (rs2.next) {
          val measurement = new Measurement()
          measurement.setMeasurementID(rs2.getString(1))
          measurement.setMeasurementName(rs2.getString(2))
          measurement.setMeasurementDType(rs2.getString(3))
          measurementlist += measurement
        }
        /*println("product's measurements = ")
        measurementlist.foreach(x => print((x.measurementID, x.measurementName, x.measurementDType)))
        println("")*/
        product.setMeasurements(measurementlist)

        product
      } finally {
        conn.close
      }
    }
    else
      throw new RuntimeException("Null connection!")
  }

  //may return multiple product
  def getProductMetaByName(productName: String, connAddr: String, user: String, password: String): ArrayBuffer[Product] = {
    val productArray = new ArrayBuffer[Product]()
    val conn = DriverManager.getConnection(connAddr, user, password)
    if (conn != null) {
      try {
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        val sql = "select product_key,product_name,platform_name,sensor_name,tile_size,cell_res,crs,level,phenomenon_time,imaging_length,imaging_width " +
          "from \"SensorLevelAndProduct\" where product_name=\'" + productName + "\';"
        val rs = statement.executeQuery(sql)
        val rsArray = new Array[String](11);
        val columnCount = rs.getMetaData().getColumnCount();

        println("Queried products:")
        while (rs.next) {
          for (i <- 1 to columnCount)
            rsArray(i - 1) = rs.getString(i)
          val productID = rsArray(0)
          val productPlatform = rsArray(2)
          val productInstrument = rsArray(3)
          val productCRS = rsArray(6)
          val productTileSize = rsArray(4)
          val productRes = rsArray(5)
          val productLevel = rsArray(7)
          val productPhenomenonTime = rsArray(8)
          val productHeight = rsArray(9)
          val productWidth = rsArray(10)

          println("productID/Key = " + productID,
            "productName = " + productName,
            "productPlatform = " + productPlatform,
            "productInstrument = " + productInstrument,
            "productCRS = " + productCRS,
            "productTileSize = " + productTileSize,
            "productRes = " + productRes,
            "productLevel = " + productLevel,
            "productPhenomenonTime = " + productPhenomenonTime,
            "productHeight = " + productHeight,
            "productWidth = " + productWidth)

          val product = Product(productID, productName, productPlatform, productInstrument, productCRS,
            productTileSize, productRes, productLevel, productPhenomenonTime, productHeight, productWidth)

          val sql2 = "select measurement_key, measurement_name, dtype " +
            "from \"MeasurementsAndProduct\" where product_key=" + productID + ";"
          val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
          val rs2 = statement.executeQuery(sql2)
          val columnCount2 = rs2.getMetaData().getColumnCount()
          val measurementlist = new ArrayBuffer[Measurement]()
          while (rs2.next) {
            val measurement = new Measurement()
            measurement.setMeasurementID(rs2.getString(1))
            measurement.setMeasurementName(rs2.getString(2))
            measurement.setMeasurementDType(rs2.getString(3))
            measurementlist += measurement
          }
          println("product's measurements = ")
          measurementlist.foreach(x => print((x.measurementID, x.measurementName, x.measurementDType)))
          println("")
          product.setMeasurements(measurementlist)

          productArray.append(product)
        }
        productArray
      } finally {
        conn.close
      }
    }
    else
      throw new RuntimeException("Null connection!")
  }

  def getAllProducts(connAddr: String, user: String, password: String):ArrayBuffer[Product]={
    val productArray = new ArrayBuffer[Product]()
    val conn = DriverManager.getConnection(connAddr, user, password)
    if (conn != null) {
      try {
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        val sql = "select product_key,product_name,platform_name,sensor_name,tile_size,cell_res,crs,level,phenomenon_time,imaging_length,imaging_width " +
          "from \"SensorLevelAndProduct\" " + ";"
        val rs = statement.executeQuery(sql)
        val rsArray = new Array[String](11);
        val columnCount = rs.getMetaData().getColumnCount();

        println("Queried products:")
        while (rs.next) {
          for (i <- 1 to columnCount)
            rsArray(i - 1) = rs.getString(i)
          val productID = rsArray(0)
          val productName = rsArray(1)
          val productPlatform = rsArray(2)
          val productInstrument = rsArray(3)
          val productCRS = rsArray(6)
          val productTileSize = rsArray(4)
          val productRes = rsArray(5)
          val productLevel = rsArray(7)
          val productPhenomenonTime = rsArray(8)
          val productHeight = rsArray(9)
          val productWidth = rsArray(10)

          println("productID/Key = " + productID,
            "productName = " + productName,
            "productPlatform = " + productPlatform,
            "productInstrument = " + productInstrument,
            "productCRS = " + productCRS,
            "productTileSize = " + productTileSize,
            "productRes = " + productRes,
            "productLevel = " + productLevel,
            "productPhenomenonTime = " + productPhenomenonTime,
            "productHeight = " + productHeight,
            "productWidth = " + productWidth)

          val product = Product(productID, productName, productPlatform, productInstrument, productCRS,
            productTileSize, productRes, productLevel, productPhenomenonTime, productHeight, productWidth)

          val sql2 = "select measurement_key, measurement_name, dtype " +
            "from \"MeasurementsAndProduct\" where product_key=" + productID + ";"
          val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
          val rs2 = statement.executeQuery(sql2)
          val columnCount2 = rs2.getMetaData().getColumnCount()
          val measurementlist = new ArrayBuffer[Measurement]()
          while (rs2.next) {
            val measurement = new Measurement()
            measurement.setMeasurementID(rs2.getString(1))
            measurement.setMeasurementName(rs2.getString(2))
            measurement.setMeasurementDType(rs2.getString(3))
            measurementlist += measurement
          }
          println("product's measurements = ")
          measurementlist.foreach(x => print((x.measurementID, x.measurementName, x.measurementDType)))
          println("")
          product.setMeasurements(measurementlist)

          productArray.append(product)
        }
        productArray
      } finally {
        conn.close
      }
    }
    else
      throw new RuntimeException("Null connection!")
  }

  def getProductMetaByParams(p: QueryParams, connAddr: String, user: String, password: String): ArrayBuffer[Product] = {
    val conn = DriverManager.getConnection(connAddr, user, password)
    //Temporal params
    val startTime = p.getStartTime
    val endTime = p.getEndTime

    //Spatial params
    val gridCodes = p.getGridCodes
    val cityCodes = p.getCityCodes
    val cityNames = p.getCityNames
    val provinceName = p.getProvinceName
    val districtName = p.getDistrictName

    //CRS, tile size, resolution and level params
    val crs = p.getCRS
    val tileSize = p.getTileSize
    val cellRes = p.getCellRes
    val level = p.getLevel

    //Cloud params
    val cloud = p.getCloudMax
    val cloudShadow = p.getCloudShadowMax

    //Product params
    val rasterProductName = p.getRasterProductName
    val platform = p.getPlatform
    val instrument = p.getInstruments

    //Measurement params
    val measurements = p.getMeasurements

    val message = new StringBuilder
    if (conn != null) {
      try {
        // Configure to be Read Only
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)

        // Extent dimension
        val extentsql = new StringBuilder;
        extentsql ++= "Select extent_key from gc_extent where 1=1 "
        if (gridCodes.length != 0) {
          extentsql ++= "AND grid_code IN ("
          for (grid <- gridCodes) {
            extentsql ++= "\'"
            extentsql ++= grid
            extentsql ++= "\',"
          }
          extentsql.deleteCharAt(extentsql.length - 1)
          extentsql ++= ")"
        }
        if (cityCodes.length != 0) {
          extentsql ++= "AND city_code IN ("
          for (city <- cityCodes) {
            extentsql ++= "\'"
            extentsql ++= city
            extentsql ++= "\',"
          }
          extentsql.deleteCharAt(extentsql.length - 1)
          extentsql ++= ")"
        }
        if (cityNames.length != 0) {
          extentsql ++= "AND city_name IN ("
          for (cityname <- cityNames) {
            extentsql ++= "\'"
            extentsql ++= cityname
            extentsql ++= "\',"
          }
          extentsql.deleteCharAt(extentsql.length - 1)
          extentsql ++= ")"
        }
        if (provinceName != "") {
          extentsql ++= "AND province_name ="
          extentsql ++= "\'"
          extentsql ++= provinceName
          extentsql ++= "\'"
        }
        if (districtName != "") {
          extentsql ++= "AND district_name ="
          extentsql ++= "\'"
          extentsql ++= districtName
          extentsql ++= "\'"
        }

        val extentResults = statement.executeQuery(extentsql.toString());
        val extentKeys = new StringBuilder;
        if (extentResults.first()) {
          extentResults.previous()
          extentKeys ++= "("
          while (extentResults.next) {
            extentKeys ++= "\'"
            extentKeys ++= extentResults.getString(1)
            extentKeys ++= "\',"
          }
          extentKeys.deleteCharAt(extentKeys.length - 1)
          extentKeys ++= ")"
        } else {
          message ++= "No products in the query extent！"
        }
        println("Extent Query SQL: " + extentsql)
        println("Extent Keys :" + extentKeys)

        //Product dimension
        val productsql = new StringBuilder;
        productsql ++= "Select DISTINCT product_key from \"SensorLevelAndProduct\" where 1=1 "
        if (instrument.length != 0) {
          productsql ++= "AND sensor_name IN ("
          for (ins <- instrument) {
            productsql ++= "\'"
            productsql ++= ins
            productsql ++= "\',"
          }
          productsql.deleteCharAt(productsql.length - 1)
          productsql ++= ")"
        }
        if (rasterProductName != "") {
          productsql ++= "AND product_name ="
          productsql ++= "\'"
          productsql ++= rasterProductName
          productsql ++= "\'"
        }
        if (platform != "") {
          productsql ++= "AND platform_name ="
          productsql ++= "\'"
          productsql ++= platform
          productsql ++= "\'"
        }
        if (crs != "") {
          productsql ++= "AND crs ="
          productsql ++= "\'"
          productsql ++= crs
          productsql ++= "\'"
        }
        if (tileSize != "") {
          productsql ++= "AND tilesize ="
          productsql ++= "\'"
          productsql ++= tileSize
          productsql ++= "\'"
        }
        if (cellRes != "") {
          productsql ++= "AND cellres ="
          productsql ++= "\'"
          productsql ++= cellRes
          productsql ++= "\'"
        }
        if (level != "") {
          productsql ++= "AND level ="
          productsql ++= "\'"
          productsql ++= level
          productsql ++= "\'"
        }
        if (startTime != "") {
          productsql ++= "AND phenomenon_time>"
          productsql ++= "\'"
          productsql ++= startTime
          productsql ++= "\'"
        }
        if (endTime != "") {
          productsql ++= "AND phenomenon_time<"
          productsql ++= "\'"
          productsql ++= endTime
          productsql ++= "\'"
        }
        val productResults = statement.executeQuery(productsql.toString());
        val productKeys = new StringBuilder;
        if (productResults.first()) {
          productResults.previous()
          productKeys ++= "("
          while (productResults.next) {
            productKeys ++= "\'"
            productKeys ++= productResults.getString(1)
            productKeys ++= "\',"
          }
          productKeys.deleteCharAt(productKeys.length - 1)
          productKeys ++= ")"
        } else {
          message ++= "No products in the query product: " + rasterProductName
          //println(message)
        }
        println("Product Query SQL: " + productsql)
        println("Product Keys :" + productKeys)

        //Quality dimension
        val qualitysql = new StringBuilder
        qualitysql ++= "Select tile_quality_key from gc_tile_quality where 1=1 "
        if (cloud != "") {
          qualitysql ++= "AND cloud<"
          qualitysql ++= cloud
        }
        if (cloudShadow != "") {
          qualitysql ++= "AND cloudShadow<"
          qualitysql ++= cloudShadow
        }

        val qualityResults = statement.executeQuery(qualitysql.toString());
        val qualityKeys = new StringBuilder;
        if (qualityResults.first()) {
          qualityResults.previous()
          qualityKeys ++= "("
          while (qualityResults.next) {
            qualityKeys ++= "\'"
            qualityKeys ++= qualityResults.getString(1)
            qualityKeys ++= "\',"
          }
          qualityKeys.deleteCharAt(qualityKeys.length - 1)
          qualityKeys ++= ")"
        } else {
          message ++= "No products in the query cloud conditions！"
          //println(message)
        }
        println("Quality Query SQL: " + qualitysql)
        println("Quality Keys :" + qualityKeys)

        //Measurement dimension
        val measurementsql = new StringBuilder
        measurementsql ++= "Select measurement_key from gc_measurement where 1=1 "
        if (measurements.length != 0) {
          measurementsql ++= "AND measurement_name IN ("
          for (measure <- measurements) {
            measurementsql ++= "\'"
            measurementsql ++= measure
            measurementsql ++= "\',"
          }
          measurementsql.deleteCharAt(measurementsql.length - 1)
          measurementsql ++= ")"
        }

        val measurementResults = statement.executeQuery(measurementsql.toString());
        val measurementKeys = new StringBuilder;
        if (measurementResults.first()) {
          measurementResults.previous()
          measurementKeys ++= "("
          while (measurementResults.next) {
            measurementKeys ++= "\'"
            measurementKeys ++= measurementResults.getString(1)
            measurementKeys ++= "\',"
          }
          measurementKeys.deleteCharAt(measurementKeys.length - 1)
          measurementKeys ++= ")"
        } else {
          message ++= "No products in the query measurements！"
          println(message)
        }
        println("Measurement Query SQL: " + measurementsql)
        println("Measurement Keys :" + measurementKeys)

        val command = "Select distinct product_key from gc_raster_tile_fact where extent_key IN" +
          extentKeys.toString() + "AND product_key IN" + productKeys.toString() + "AND tile_quality_key IN" +
          qualityKeys.toString() + "AND measurement_key IN" + measurementKeys.toString() + ";"

        println("Product Query SQL:" + command)

        val productKeysResults = statement.executeQuery(command)
        val productKeysArray= new ArrayBuffer[String]()
        val productArray= new ArrayBuffer[Product]()
        if(productKeysResults.first()){
          productKeysResults.previous()
          while(productKeysResults.next()){
            productKeysArray.append(productKeysResults.getString(1))
          }
        } else{
          message++="product not found！"
        }
        for(key <- productKeysArray){
          val product = getProductMetaByKey(key, connAddr, user, password)
          productArray.append(product)
        }
        productArray
      } finally {
        conn.close
      }
    } else
      throw new RuntimeException("connection failed")
  }

}

