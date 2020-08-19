package whu.edu.cn.query.service

import java.sql.{DriverManager, ResultSet}
import java.text.SimpleDateFormat

import com.google.gson.{JsonObject, JsonParser}
import geotrellis.layer.{Bounds, LayoutDefinition, SpaceTimeKey, TileLayerMetadata}
import geotrellis.proj4.CRS
import geotrellis.raster.{CellType, Tile, TileLayout}
import org.apache.log4j.BasicConfigurator
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.io.WKTReader
import whu.edu.cn.query.entity.Measurement.getMeasurementMetaByMeaAndProKey
import whu.edu.cn.query.entity.Product.getProductMetaByKey
import whu.edu.cn.query.entity.{RasterTile, RasterTileLayerMetadata, SpaceTimeBandKey}
import whu.edu.cn.query.entity
import whu.edu.cn.query.entity.{QueryParams, RasterTile, RasterTileLayerMetadata, SpaceTimeBandKey}
import whu.edu.cn.query.util.HbaseUtil.{getTileCell, getTileMeta}
import whu.edu.cn.query.util.TileSerializer.deserializeTileData

import scala.collection.mutable.ArrayBuffer

object QueryRasterTiles {
  val conn_str = "jdbc:postgresql://125.220.153.26:5432/geocube"
  Class.forName("org.postgresql.Driver")
  BasicConfigurator.configure()

  def main(args: Array[String]): Unit = {

    /*//Test query product by name
    getProductMetaByName("LC08_L1TP_GDAL", conn_str, "geocube", "ypfamily608")*/

    /*//Test get all products
    getAllProducts(conn_str, "geocube", "ypfamily608")*/

    /*//Test query product by params:spatial, measurement, quality, product
    val queryGeometry = new GeometryFactory().createPolygon(Array(
      new Coordinate(119.46494046724021, 32.073457222586285),
      new Coordinate(119.46494046724021, 34.2597805438586),
      new Coordinate(122.02181165740333, 34.2597805438586),
      new Coordinate(122.02181165740333, 32.073457222586285),
      new Coordinate(119.46494046724021, 32.073457222586285)
    ))
    val extent = geotrellis.vector.Extent(-180, -90, 180, 90)
    val tl = TileLayout(360, 180, 1024, 1024)
    val ld = LayoutDefinition(extent, tl)
    val gridCodeArray = geom2GridCode(queryGeometry, ld.layoutCols, ld.layoutRows, ld.extent)
    gridCodeArray.foreach(println(_))
    val gridCodes = gridCodeArray

    val measurements = ArrayBuffer("Red","Pan") //ArrayBuffer("Near-Infrared","Blue")

    val cloud = "100"

    val level = "9"

    val p = new SearchParams("",measurements,gridCodes,cloud,level)
    p.setStarttimeParam("2018-01-20 00:30:46.223")
    p.setEndtimeParam("2018-04-25 00:30:46.335")
    getProductMetaByParams(p, conn_str, "geocube", "ypfamily608")*/

    //Test query tiles
    val queryBegin = System.currentTimeMillis()

    val queryParams = new QueryParams
    queryParams.setRasterProductNames(Array("LC08_L1TP_ARD_EO", "LC8_L1TP_TMS_EO"))
    queryParams.setExtent(112.46494046724021, 29.073457222586285, 115.02181165740333, 31.2597805438586)
    queryParams.setTime("2013-01-01 02:30:59.415", "2019-01-01 02:30:59.41")
    queryParams.setMeasurements(Array("Green", "Near-Infrared"))

    //queryParams.setLevel("999") //默认为999
    val tileLayerArrayWithMeta:(Array[(SpaceTimeBandKey, Tile)],RasterTileLayerMetadata[SpaceTimeKey]) = getTiles(queryParams)
    println(tileLayerArrayWithMeta._2.getRasterProductNames)
    println(tileLayerArrayWithMeta._2.getTileLayerMetadata)
    val queryEnd = System.currentTimeMillis()

    println("Query time of " + tileLayerArrayWithMeta._1.length + " raster tiles: " + (queryEnd - queryBegin))


    /*//Check datas
    val tileLayerRddWithMeta:(RDD[(SpaceTimeKey,(String,Tile))],TileLayerMetadata[SpaceTimeKey]) = getTileLayerRddWithMeta(sc, p)
    val resultsRdd = tileLayerRddWithMeta._1.map(x=>(x._1.spatialKey,x._2._2))
    val spatialKeyBounds = tileRdd._2.bounds.get.toSpatial
    val spatialMetadata = TileLayerMetadata(
      tileRdd._2.cellType,
      tileRdd._2.layout,
      tileRdd._2.extent,
      tileRdd._2.crs,
      spatialKeyBounds)
    val stitchedRdd:TileLayerRDD[SpatialKey] = ContextRDD(resultsRdd, spatialMetadata)
    val stitched: Raster[Tile] = stitchedRdd.stitch()
    GeoTiff(stitched, stitchedRdd.metadata.crs).write("/home/geocube/environment_test/TileQuery_Env/" + gridCodes + ".TIF")*/

    println("Query and analisys finished!")
  }

  def getTiles(implicit sc: SparkContext, p: QueryParams): (RDD[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = {
    val tileArray = getTiles(p)
    val rdd = sc.parallelize(tileArray._1)
    (rdd, tileArray._2)
  }

  def getTiles(p: QueryParams):(Array[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = {
    if (p.getRasterProductNames.length == 0) {
      getTileLayerArrayWithMeta(p)
    } else {
      val multiProductTileLayerArray= ArrayBuffer[Array[(SpaceTimeBandKey, Tile)]]()
      val multiProductTileLayerMeta = ArrayBuffer[RasterTileLayerMetadata[SpaceTimeKey]]()
      for(rasterProductName <- p.getRasterProductNames){
        p.setRasterProductName(rasterProductName)
        val results = getTileLayerArrayWithMeta(p)
        if(results != null){
          multiProductTileLayerArray.append(results._1)
          multiProductTileLayerMeta.append(results._2)
        }
      }
      if(multiProductTileLayerMeta.length < 1) throw new RuntimeException("No tiles with the query condition!")
      var destTileLayerMetaData = multiProductTileLayerMeta(0).getTileLayerMetadata
      val destRasterProductNames = ArrayBuffer[String]()
      for(i <- multiProductTileLayerMeta) {
        destTileLayerMetaData = destTileLayerMetaData.merge(i.getTileLayerMetadata)
        destRasterProductNames.append(i.getRasterProductName)
      }
      val destRasterTileLayerMetaData = entity.RasterTileLayerMetadata[SpaceTimeKey](destTileLayerMetaData, _rasterProductNames = destRasterProductNames)
      (multiProductTileLayerArray.toArray.flatten, destRasterTileLayerMetaData)
    }
  }


  def getTileLayerArrayWithMeta(p: QueryParams): (Array[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) = {
    val conn = DriverManager.getConnection(conn_str, "geocube", "ypfamily608")
    //Temporal params
    val startTime = p.getStartTime
    val endTime = p.getEndTime
    val nextStartTime = p.getNextStartTime
    val nextEndTime = p.getNextEndTime

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
        val extentsql = new StringBuilder
        extentsql ++= "Select extent_key from \"LevelAndExtent\" where 1=1 "

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
        if (tileSize != "") {
          extentsql ++= "AND tile_size ="
          extentsql ++= "\'"
          extentsql ++= tileSize
          extentsql ++= "\'"
        }
        if (cellRes != "") {
          extentsql ++= "AND cell_res ="
          extentsql ++= "\'"
          extentsql ++= cellRes
          extentsql ++= "\'"
        }
        if (level != "") {
          extentsql ++= "AND level ="
          extentsql ++= "\'"
          extentsql ++= level
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
          return null
          //throw new RuntimeException("No tiles in the query extent！")
          //message ++= "No tiles in the query extent！"
        }
        println("Extent Query SQL: " + extentsql)
        println("Extent Keys :" + extentKeys)

        /*//level dimension, 和extent表关联构建视图可放到extent dimension中查询
        val levelsql = new StringBuilder
        levelsql ++= "Select resolution_key from gc_level where 1=1 "
        if(level.length != 0){
          levelsql ++= "AND level ="
          levelsql ++= level
        }
        if(tileSize.length != 0){
          levelsql ++= " AND tile_size ="
          levelsql ++= "\'"
          levelsql ++= tileSize
          levelsql ++= "\'"
        }
        if(cellRes.length != 0){
          levelsql ++= " AND cell_res ="
          levelsql ++= "\'"
          levelsql ++= cellRes
          levelsql ++= "\'"
        }

        val levelResults = statement.executeQuery(levelsql.toString());
        val resolutionKeys = new StringBuilder;
        if (levelResults.first()) {
          levelResults.previous()
          resolutionKeys ++= "("
          while (levelResults.next) {
            resolutionKeys ++= "\'"
            resolutionKeys ++= levelResults.getString(1)
            resolutionKeys ++= "\',"
          }
          resolutionKeys.deleteCharAt(resolutionKeys.length - 1)
          resolutionKeys ++= ")"
        } else {
          return null
          //throw new RuntimeException("No tiles in the query extent！")
          //message ++= "No tiles in the query extent！"
        }
        println("Level Query SQL: " + levelsql)
        println("Resolution Keys :" + resolutionKeys)*/

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
          productsql ++= "AND tile_size ="
          productsql ++= "\'"
          productsql ++= tileSize
          productsql ++= "\'"
        }
        if (cellRes != "") {
          productsql ++= "AND cell_res ="
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
        /*if (startTime != "") {
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
        }*/
        if(startTime != "" && endTime != ""){
          productsql ++= " AND (phenomenon_time BETWEEN "
          productsql ++= "\'"
          productsql ++= startTime
          productsql ++= "\'"
          productsql ++= " AND"
          productsql ++= "\'"
          productsql ++= endTime
          productsql ++= "\'"
        }
        if(nextStartTime != "" && nextEndTime != ""){
          productsql ++= " Or phenomenon_time BETWEEN "
          productsql ++= "\'"
          productsql ++= nextStartTime
          productsql ++= "\'"
          productsql ++= " AND"
          productsql ++= "\'"
          productsql ++= nextEndTime
          productsql ++= "\')"
        } else{
          productsql ++= ")"
        }

        println(productsql.toString())
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
          return null
          //throw new RuntimeException("No tiles in the query product: " + rasterProductName)
          //message ++= "No tiles in the query product: " + rasterProductName
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
          return null
          //throw new RuntimeException("No tiles in the query cloud conditions！")
          //message ++= "No tiles in the query cloud conditions！"
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
          return null
          //throw new RuntimeException("No tiles in the query measurements！")
          //message ++= "No tiles in the query measurements！"
        }
        println("Measurement Query SQL: " + measurementsql)
        println("Measurement Keys :" + measurementKeys)

        val command = "Select tile_data_id,product_key,measurement_key,extent_key,tile_quality_key from gc_raster_tile_fact where extent_key IN" +
          extentKeys.toString() + "AND product_key IN" + productKeys.toString() + "AND tile_quality_key IN" +
          qualityKeys.toString() + "AND measurement_key IN" + measurementKeys.toString() + ";"
        println("Raster Tile Fact Query SQL:" + command)

        val tileIDResults = statement.executeQuery(command)
        val tileAndDimensionKeys = new ArrayBuffer[Array[String]]()
        if (tileIDResults.first()) {
          tileIDResults.previous()
          while (tileIDResults.next()) {
            val keyArray = new Array[String](5)
            keyArray(0) = tileIDResults.getString(1)
            keyArray(1) = tileIDResults.getString(2)
            keyArray(2) = tileIDResults.getString(3)
            keyArray(3) = tileIDResults.getString(4)
            keyArray(4) = tileIDResults.getString(5)
            tileAndDimensionKeys.append(keyArray)
          }
        } else {
          println("No tiles of " + rasterProductName + " acquired!")
        }

        val queriedRasterTiles = ArrayBuffer[RasterTile]()
        tileAndDimensionKeys.foreach(keys => queriedRasterTiles.append(initRasterTile(keys(0), keys(1), keys(2), keys(3), keys(4))))

        println("Return " + queriedRasterTiles.length + " tiles of " + rasterProductName + " product: ")
        queriedRasterTiles.foreach(x=>print("{tile:{ID:" + x.ID + ", ProductID:" + x.productID + "}}"))
        println()
        if(queriedRasterTiles.length != 0) makeLayerArrayWithMeta(queriedRasterTiles)
        else null
      } finally {
        conn.close
      }
    } else
      throw new RuntimeException("connection failed")
  }

  def initRasterTile(ID: String, productKey: String, MeasurmentKey: String, ExtentKey: String, QualityKey: String): RasterTile = {
    val rasterTile = RasterTile(ID, productKey)
    //println("<********TileID/Key = " + ID + "********>")
    //Tile meta stored in postgre
    val productMeta = getProductMetaByKey(productKey, conn_str, "geocube", "ypfamily608")
    val measurementMeta = getMeasurementMetaByMeaAndProKey(MeasurmentKey, productKey, conn_str, "geocube", "ypfamily608")
    //val extentMeta = getExtentMetaByKey(ExtentKey, conn_str, "geocube", "ypfamily608")
    //val qualitymeta = getTileQualityMetaByKey(QualityKey, conn_str, "geocube", "ypfamily608")

    rasterTile.setProductMeta(productMeta)
    rasterTile.setMeasurement(measurementMeta)
    //rasterTile.setExtentMeta(extentMeta)
    //rasterTile.setTileQualityMeta(qualitymeta)

    //Tile bytes and meta stored in HBase
    val tileMeta = getTileMeta("hbase_raster", ID, "rasterData", "metaData")
    val tileBytes = getTileCell("hbase_raster", ID, "rasterData", "tile")

    val json = new JsonParser()
    val obj = json.parse(tileMeta).asInstanceOf[JsonObject]
    rasterTile.setCRS(obj.get("cRS").toString)
    rasterTile.setColNum(obj.get("column").toString)
    rasterTile.setRowNum(obj.get("row").toString)

    val trueDataWKT = obj.get("trueDataWKT").toString.replace("\"", "")
    val reader = new WKTReader
    val polygon = reader.read(trueDataWKT)
    val envelope = polygon.getEnvelopeInternal
    rasterTile.setLeftBottomLong(envelope.getMinX.toString)
    rasterTile.setLeftBottomLat(envelope.getMinY.toString)
    rasterTile.setRightUpperLong(envelope.getMaxX.toString)
    rasterTile.setRightUpperLat(envelope.getMaxY.toString)

    val dType = obj.get("cellType").toString.replace("\"", "")
    //assert(obj.get("cellType").toString.replace("\"", "") == rasterTile.getMeasurement.getMeasurementDType)

    /* println("Other meta of the queried tile stored in HBase:")
     println("tileCRS:" + rasterTile.CRS,
       "dType:" + dType,
       "(column,row):" + (rasterTile.colNum, rasterTile.rowNum),
       "trueDataRange:" + (rasterTile.leftBottomLong, rasterTile.leftBottomLat, rasterTile.rightUpperLong, rasterTile.rightUpperLat))*/

    val tileData = deserializeTileData(rasterTile.getProductMeta.getPlatform, tileBytes, rasterTile.getProductMeta.tilesize.toInt, dType)
    //val tileData = deserializeTileData(rasterTile.getProductMeta.getPlatform, tileBytes, 1024, dType)
    rasterTile.setData(tileData)
    rasterTile
  }


  def makeLayerArrayWithMeta(rasterTiles:ArrayBuffer[RasterTile]):(Array[(SpaceTimeBandKey, Tile)], RasterTileLayerMetadata[SpaceTimeKey]) ={
    val layer:ArrayBuffer[(SpaceTimeBandKey,Tile)] = makeLayer(rasterTiles)
    val productName: String =rasterTiles(0).getProductMeta.productName
    val extent = geotrellis.vector.Extent(-180, -90, 180, 90)
    val tileSize: Int = rasterTiles(0).getProductMeta.tilesize.toInt
    val tl = TileLayout(360, 180, tileSize, tileSize)
    val ld = LayoutDefinition(extent, tl)

    val colArray = new ArrayBuffer[Int]()
    val rowArray = new ArrayBuffer[Int]()
    val longArray = new ArrayBuffer[Double]()
    val latArray =new ArrayBuffer[Double]()
    val instantArray = new ArrayBuffer[Long]()

    for(tile <- rasterTiles){
      colArray.append(Integer.parseInt(tile.getColNum))
      rowArray.append(Integer.parseInt(tile.getRowNum))
      longArray.append(tile.getLeftBottomLong.toDouble)
      longArray.append(tile.getRightUpperLong.toDouble)
      latArray.append(tile.getLeftBottomLat.toDouble)
      latArray.append(tile.getRightUpperLat.toDouble)
      instantArray.append(new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" ).parse(tile.getProductMeta.getPhenomenonTime).getTime)
    }

    val (minCol,maxCol)  = (colArray.min, colArray.max)
    val (minRow,maxRow)  = (rowArray.min, rowArray.max)
    val (minLong,maxLong)  = (longArray.min, longArray.max)
    val (minLat,maxLat)  = (latArray.min, latArray.max)
    val (minInstant,maxInstant)  = (instantArray.min, instantArray.max)

    val bounds = Bounds(SpaceTimeKey(minCol,minRow,minInstant),SpaceTimeKey(maxCol,maxRow,maxInstant))
    val actualExtent = geotrellis.vector.Extent(minLong, minLat, maxLong, maxLat)
    val dtype = rasterTiles.last.getMeasurement.getMeasurementDType
    val celltype = CellType.fromName(dtype)
    val crsStr = rasterTiles.last.getCRS.toString.replace("\"", "")
    var crs:CRS = CRS.fromEpsgCode(4326)
    if(crsStr == "WGS84"){
      crs = CRS.fromEpsgCode(4326)
    }

    val targetArray = (layer.toArray,entity.RasterTileLayerMetadata(TileLayerMetadata(celltype,ld,actualExtent,crs,bounds), productName))
    /*println("Queried tiles info:")
    targetArray._1.foreach(x=>println(x._1, x._2._1, x._2._2.rows, x._2._2.cols, x._2._2.cellType))*/
    targetArray
  }


  def makeLayer(rasterTiles: ArrayBuffer[RasterTile]): ArrayBuffer[(SpaceTimeBandKey, Tile)] = {
    val layer = ArrayBuffer[(SpaceTimeBandKey, Tile)]()
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    for (tile <- rasterTiles) {
      val phenomenontime = sdf.parse(tile.getProductMeta.getPhenomenonTime).getTime
      val measurement = tile.getMeasurement.getMeasurementName
      val colnum = Integer.parseInt(tile.getColNum)
      val rownum = Integer.parseInt(tile.getRowNum)
      val Tile = tile.getData
      val k = entity.SpaceTimeBandKey(SpaceTimeKey(colnum, rownum, phenomenontime), measurement)
      val v = Tile
      val l = (k, v)
      layer.append(l)
    }
    layer
  }

  def getPyramidTile(p: QueryParams): Array[Byte] = {
    val conn = DriverManager.getConnection(conn_str, "geocube", "ypfamily608")
    //Temporal params
    val startTime = p.getStartTime
    val endTime = p.getEndTime
    val nextStartTime = p.getNextStartTime
    val nextEndTime = p.getNextEndTime

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
        val extentsql = new StringBuilder
        extentsql ++= "Select extent_key from \"LevelAndExtent\" where 1=1 "

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
        if (tileSize != "") {
          extentsql ++= "AND tile_size ="
          extentsql ++= "\'"
          extentsql ++= tileSize
          extentsql ++= "\'"
        }
        if (cellRes != "") {
          extentsql ++= "AND cell_res ="
          extentsql ++= "\'"
          extentsql ++= cellRes
          extentsql ++= "\'"
        }
        if (level != "") {
          extentsql ++= "AND level ="
          extentsql ++= "\'"
          extentsql ++= level
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
          return null
          //throw new RuntimeException("No tiles in the query extent！")
          //message ++= "No tiles in the query extent！"
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
          productsql ++= "AND tile_size ="
          productsql ++= "\'"
          productsql ++= tileSize
          productsql ++= "\'"
        }
        if (cellRes != "") {
          productsql ++= "AND cell_res ="
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
        /*if (startTime != "") {
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
        }*/
        if(startTime != "" && endTime != ""){
          productsql ++= " AND (phenomenon_time BETWEEN "
          productsql ++= "\'"
          productsql ++= startTime
          productsql ++= "\'"
          productsql ++= " AND"
          productsql ++= "\'"
          productsql ++= endTime
          productsql ++= "\'"
        }
        if(nextStartTime != "" && nextEndTime != ""){
          productsql ++= " Or phenomenon_time BETWEEN "
          productsql ++= "\'"
          productsql ++= nextStartTime
          productsql ++= "\'"
          productsql ++= " AND"
          productsql ++= "\'"
          productsql ++= nextEndTime
          productsql ++= "\')"
        } else{
          productsql ++= ")"
        }

        println(productsql.toString())
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
          return null
          //throw new RuntimeException("No tiles in the query product: " + rasterProductName)
          //message ++= "No tiles in the query product: " + rasterProductName
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
          return null
          //throw new RuntimeException("No tiles in the query cloud conditions！")
          //message ++= "No tiles in the query cloud conditions！"
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
          return null
          //throw new RuntimeException("No tiles in the query measurements！")
          //message ++= "No tiles in the query measurements！"
        }
        println("Measurement Query SQL: " + measurementsql)
        println("Measurement Keys :" + measurementKeys)

        val command = "Select tile_data_id,product_key,measurement_key,extent_key,tile_quality_key from gc_raster_tile_fact where extent_key IN" +
          extentKeys.toString() + "AND product_key IN" + productKeys.toString() + "AND tile_quality_key IN" +
          qualityKeys.toString() + "AND measurement_key IN" + measurementKeys.toString() + ";"
        println("Raster Tile Fact Query SQL:" + command)

        val tileIDResults = statement.executeQuery(command)
        val tileAndDimensionKeys = new ArrayBuffer[Array[String]]()
        if (tileIDResults.first()) {
          tileIDResults.previous()
          while (tileIDResults.next()) {
            val keyArray = new Array[String](5)
            keyArray(0) = tileIDResults.getString(1)
            keyArray(1) = tileIDResults.getString(2)
            keyArray(2) = tileIDResults.getString(3)
            keyArray(3) = tileIDResults.getString(4)
            keyArray(4) = tileIDResults.getString(5)
            tileAndDimensionKeys.append(keyArray)
          }
        } else {
          println("No tiles of " + rasterProductName + " acquired!")
        }

        val tileDataID = tileAndDimensionKeys(0)(0)
        val tilePngBytes = getTileCell("hbase_raster", tileDataID, "rasterData", "tile")
        println("Returned " + tileAndDimensionKeys.length + " tiles of " + rasterProductName + " product")
        tilePngBytes
      } finally {
        conn.close
      }
    } else
      throw new RuntimeException("connection failed")
  }


}
