package whu.edu.cn.query.service

import java.sql.{DriverManager, ResultSet}
import java.text.SimpleDateFormat
import java.util.UUID

import com.google.gson.{JsonObject, JsonParser}
import geotrellis.layer.SpaceTimeKey
import org.apache.log4j.BasicConfigurator
import org.geotools.geojson.feature.FeatureJSON
import org.opengis.feature.simple.SimpleFeature
import whu.edu.cn.core.vector.rdd.GeoObject
import whu.edu.cn.query.entity.QueryParams
import whu.edu.cn.query.service.QueryRasterTiles.conn_str
import whu.edu.cn.query.util.HbaseUtil._

import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json.JSONArray
import scala.collection.JavaConversions.bufferAsJavaList;

object QueryVectorObjects{
  val conn_str = "jdbc:postgresql://125.220.153.26:5432/geocube"
  Class.forName("org.postgresql.Driver")
  BasicConfigurator.configure()

  def main(args: Array[String]): Unit = {
    val queryParams = new QueryParams
    queryParams.setVectorProductName("Hainan_Daguangba_School_Vector")
    queryParams.setExtent(108.46494046724021, 18.073457222586285, 111.02181165740333, 20.2597805438586)
    queryParams.setTime("2013-01-01 02:30:59.415", "2019-01-01 02:30:59.41")
//    getVectorObjects(queryParams);
    getVectorGeoJsons2(queryParams);

  }

  def getVectorObjects(p: QueryParams):Array[(SpaceTimeKey, Iterable[GeoObject])] = {
    if(p.getVectorProductNames.length  == 0){
      getGridLayerGeoObjectArray(p)
    }else{
      val multiProductGridLayerGeoObjectArray= ArrayBuffer[Array[(SpaceTimeKey, Iterable[GeoObject])]]()
      for(vectorProductName <- p.getVectorProductNames){
        p.setVectorProductName(vectorProductName)
        val results = getGridLayerGeoObjectArray(p)
        if(results != null)
          multiProductGridLayerGeoObjectArray.append(results)
      }
      if(multiProductGridLayerGeoObjectArray.length < 1) throw new RuntimeException("No vectors with the query condition!")
      multiProductGridLayerGeoObjectArray.flatten.groupBy(_._1).toArray.map{x =>
        val geoObjects:ArrayBuffer[GeoObject] = x._2.map(_._2).flatten
        (x._1, geoObjects)
      }
    }

  }

  def getGridLayerGeoObjectArray(p: QueryParams):Array[(SpaceTimeKey, Iterable[GeoObject])] = {
    val conn = DriverManager.getConnection(conn_str, "geocube", "ypfamily608")
    //Product params
    val vectorProductName = p.getVectorProductName

    //Temporal params
    val startTime = p.getStartTime
    val endTime = p.getEndTime

    //Spatial params
    val gridCodes = p.getGridCodes
    val cityCodes = p.getCityCodes
    val cityNames = p.getCityNames
    val provinceName = p.getProvinceName
    val districtName = p.getDistrictName

    //CRS
    val crs = p.getCRS

    if (conn != null) {
      try{
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)

        // Extent dimension
        val extentsql = new StringBuilder
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
          return null
          //throw new RuntimeException("No tiles in the query extent！")
          //message ++= "No tiles in the query extent！"
        }
        println("Extent Query SQL: " + extentsql)
        println("Extent Keys :" + extentKeys)

        //Product dimension
        val productsql = new StringBuilder;
        productsql ++= "Select DISTINCT product_key,phenomenon_time from \"gc_product\" where 1=1 "
        if (vectorProductName != "") {
          productsql ++= "AND product_name ="
          productsql ++= "\'"
          productsql ++= vectorProductName
          productsql ++= "\'"
        }
        if (crs != "") {
          productsql ++= "AND crs ="
          productsql ++= "\'"
          productsql ++= crs
          productsql ++= "\'"
        }
        if(startTime != "" && endTime != ""){
          productsql ++= " AND (phenomenon_time BETWEEN "
          productsql ++= "\'"
          productsql ++= startTime
          productsql ++= "\'"
          productsql ++= " AND"
          productsql ++= "\'"
          productsql ++= endTime
          productsql ++= "\')"
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

        val command = "Select tile_data_id,product_key,extent_key from gc_vector_tile_fact where extent_key IN" +
          extentKeys.toString() + "AND product_key IN" + productKeys.toString() + ";"
        println("Vector Fact Query SQL:" + command)

        val geoObjectsIDResults = statement.executeQuery(command)
        val geoObjectsAndDimensionKeys = new ArrayBuffer[Array[String]]()
        if (geoObjectsIDResults.first()) {
          geoObjectsIDResults.previous()
          while (geoObjectsIDResults.next()) {
            val keyArray = new Array[String](3)
            keyArray(0) = geoObjectsIDResults.getString(1)
            keyArray(1) = geoObjectsIDResults.getString(2)
            keyArray(2) = geoObjectsIDResults.getString(3)
            geoObjectsAndDimensionKeys.append(keyArray)
          }
        } else {
          println("No vector objects of " + vectorProductName + " acquired!")
        }

        val GridLayerGeoObjectArray: ArrayBuffer[(SpaceTimeKey, Iterable[GeoObject])] = new ArrayBuffer[(SpaceTimeKey, Iterable[GeoObject])]()
        val fjson = new FeatureJSON()
        geoObjectsAndDimensionKeys.foreach{ keys =>
          println(keys(0))
          val listString = keys(0)
          val geoObjectKeys = listString.substring(listString.indexOf("(")+1,listString.indexOf(")")).split(", ")

          val productKey = keys(1)
          val sql = "select phenomenon_time from gc_product where product_key=" + productKey + ";"
          val rs = statement.executeQuery(sql)
          //矢量的时间
          var time:Long ="20200101".toLong
          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          if (rs.first()) {
            rs.previous()
            while (rs.next()) {
              time = sdf.parse(rs.getString(1)).getTime
            }
          } else {
            println("No vector time acquired!")
          }
          //获取第一个矢量的瓦片元信息得到col和row
          val col_row =getVectorMeta("hbase_vector", geoObjectKeys(0), "vectorData", "tilesMetaData").dropRight(1).substring(1)
          val json = new JsonParser()
          val obj = json.parse(col_row).asInstanceOf[JsonObject]

          val extent = json.parse(obj.get("extent").toString).asInstanceOf[JsonObject]

          val (col, row) = (extent.get("column").toString.toInt, extent.get("row").toString.toInt)
//          val (col, row) = (290, 70)
          val geoObjects: ArrayBuffer[GeoObject] = new ArrayBuffer[GeoObject]()
          geoObjectKeys.foreach{geoObjectkey =>
            println(geoObjectkey)
            val featureStr = getVectorMeta("hbase_vector", geoObjectkey, "vectorData", "metaData")
            val feature:SimpleFeature = fjson.readFeature(featureStr)
            val uuid = UUID.randomUUID().toString
            val geoObject = new GeoObject(uuid, feature)
            geoObjects.append(geoObject)
          }
//          println(geoObjects)
//          println(SpaceTimeKey(col, row, time))
          GridLayerGeoObjectArray.append((SpaceTimeKey(col, row, time), geoObjects))
        }
//        println(GridLayerGeoObjectArray.toArray)
        GridLayerGeoObjectArray.toArray

      }finally
        conn.close()
    }else
      throw new RuntimeException("connection failed")

  }


  def getVectorGeoJsons(p:QueryParams):List[JsonObject]={
    val conn = DriverManager.getConnection(conn_str, "geocube", "ypfamily608")
    //Product params
    val vectorProductName = p.getVectorProductName

    //Temporal params
    val startTime = p.getStartTime
    val endTime = p.getEndTime

    //Spatial params
    val gridCodes = p.getGridCodes
    val cityCodes = p.getCityCodes
    val cityNames = p.getCityNames
    val provinceName = p.getProvinceName
    val districtName = p.getDistrictName

    //CRS
    val crs = p.getCRS

    if (conn != null) {
      try{
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)

        // Extent dimension
        val extentsql = new StringBuilder
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
          return null
          //throw new RuntimeException("No tiles in the query extent！")
          //message ++= "No tiles in the query extent！"
        }
        println("Extent Query SQL: " + extentsql)
        println("Extent Keys :" + extentKeys)

        //Product dimension
        val productsql = new StringBuilder;
        productsql ++= "Select DISTINCT product_key,phenomenon_time from \"gc_product\" where 1=1 "
        if (vectorProductName != "") {
          productsql ++= "AND product_name ="
          productsql ++= "\'"
          productsql ++= vectorProductName
          productsql ++= "\'"
        }
        if (crs != "") {
          productsql ++= "AND crs ="
          productsql ++= "\'"
          productsql ++= crs
          productsql ++= "\'"
        }
        if(startTime != "" && endTime != ""){
          productsql ++= " AND (phenomenon_time BETWEEN "
          productsql ++= "\'"
          productsql ++= startTime
          productsql ++= "\'"
          productsql ++= " AND"
          productsql ++= "\'"
          productsql ++= endTime
          productsql ++= "\')"
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

        val command = "Select tile_data_id,product_key,extent_key from gc_vector_tile_fact where extent_key IN" +
          extentKeys.toString() + "AND product_key IN" + productKeys.toString() + ";"
        println("Vector Fact Query SQL:" + command)

        val geoObjectsIDResults = statement.executeQuery(command)
        val geoObjectsAndDimensionKeys = new ArrayBuffer[Array[String]]()
        if (geoObjectsIDResults.first()) {
          geoObjectsIDResults.previous()
          while (geoObjectsIDResults.next()) {
            val keyArray = new Array[String](3)
            keyArray(0) = geoObjectsIDResults.getString(1)
            keyArray(1) = geoObjectsIDResults.getString(2)
            keyArray(2) = geoObjectsIDResults.getString(3)
            geoObjectsAndDimensionKeys.append(keyArray)
          }
        } else {
          println("No vector objects of " + vectorProductName + " acquired!")
        }

        val VectorGeoJsonArray: ArrayBuffer[JsonObject] = new ArrayBuffer[JsonObject]()
        val fjson = new FeatureJSON()
        geoObjectsAndDimensionKeys.foreach{ keys =>
          println(keys(0))
          val listString = keys(0)
          val geoObjectKeys = listString.substring(listString.indexOf("(")+1,listString.indexOf(")")).split(", ")

          val productKey = keys(1)
          val sql = "select phenomenon_time from gc_product where product_key=" + productKey + ";"
          val rs = statement.executeQuery(sql)
          //矢量的时间
          var time:Long ="20200101".toLong
          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          if (rs.first()) {
            rs.previous()
            while (rs.next()) {
              time = sdf.parse(rs.getString(1)).getTime
            }
          } else {
            println("No vector time acquired!")
          }
          //获取第一个矢量的瓦片元信息得到col和row
          val col_row =getVectorMeta("hbase_vector", geoObjectKeys(0), "vectorData", "tilesMetaData").dropRight(1).substring(1)
          val json = new JsonParser()
          val obj = json.parse(col_row).asInstanceOf[JsonObject]
          val extent = json.parse(obj.get("extent").toString).asInstanceOf[JsonObject]
          val (col, row) = (extent.get("column").toString.toInt, extent.get("row").toString.toInt)

//          val geoJsons: ArrayBuffer[JsonObject] = new ArrayBuffer[JsonObject]()
          geoObjectKeys.foreach{geoObjectkey =>
            println(geoObjectkey)
            val metaJson = getVectorMeta("hbase_vector", geoObjectkey, "vectorData", "metaData")
            val Json = json.parse(metaJson).asInstanceOf[JsonObject]
//            geoJsons.append(Json)
            VectorGeoJsonArray.append(Json)
          }

        }

//        println(VectorGeoJsonArray.toArray)
        VectorGeoJsonArray.toList


      }finally
        conn.close()
    }else
      throw new RuntimeException("connection failed")

  }

  def getVectorGeoJsons2(p:QueryParams):java.util.List[String]={
    val conn = DriverManager.getConnection(conn_str, "geocube", "ypfamily608")
    //Product params
    val vectorProductName = p.getVectorProductName

    //Temporal params
    val startTime = p.getStartTime
    val endTime = p.getEndTime

    //Spatial params
    val gridCodes = p.getGridCodes
    val cityCodes = p.getCityCodes
    val cityNames = p.getCityNames
    val provinceName = p.getProvinceName
    val districtName = p.getDistrictName

    //CRS
    val crs = p.getCRS

    if (conn != null) {
      try{
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)

        // Extent dimension
        val extentsql = new StringBuilder
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
          return null
          //throw new RuntimeException("No tiles in the query extent！")
          //message ++= "No tiles in the query extent！"
        }
        println("Extent Query SQL: " + extentsql)
        println("Extent Keys :" + extentKeys)

        //Product dimension
        val productsql = new StringBuilder;
        productsql ++= "Select DISTINCT product_key,phenomenon_time from \"gc_product\" where 1=1 "
        if (vectorProductName != "") {
          productsql ++= "AND product_name ="
          productsql ++= "\'"
          productsql ++= vectorProductName
          productsql ++= "\'"
        }
        if (crs != "") {
          productsql ++= "AND crs ="
          productsql ++= "\'"
          productsql ++= crs
          productsql ++= "\'"
        }
        if(startTime != "" && endTime != ""){
          productsql ++= " AND (phenomenon_time BETWEEN "
          productsql ++= "\'"
          productsql ++= startTime
          productsql ++= "\'"
          productsql ++= " AND"
          productsql ++= "\'"
          productsql ++= endTime
          productsql ++= "\')"
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

        val command = "Select tile_data_id,product_key,extent_key from gc_vector_tile_fact where extent_key IN" +
          extentKeys.toString() + "AND product_key IN" + productKeys.toString() + ";"
        println("Vector Fact Query SQL:" + command)

        val geoObjectsIDResults = statement.executeQuery(command)
        val geoObjectsAndDimensionKeys = new ArrayBuffer[Array[String]]()
        if (geoObjectsIDResults.first()) {
          geoObjectsIDResults.previous()
          while (geoObjectsIDResults.next()) {
            val keyArray = new Array[String](3)
            keyArray(0) = geoObjectsIDResults.getString(1)
            keyArray(1) = geoObjectsIDResults.getString(2)
            keyArray(2) = geoObjectsIDResults.getString(3)
            geoObjectsAndDimensionKeys.append(keyArray)
          }
        } else {
          println("No vector objects of " + vectorProductName + " acquired!")
        }

//        val VectorGeoJsonArray: ArrayBuffer[JsonObject] = new ArrayBuffer[JsonObject]()
        val VectorGeoJsonArray: ArrayBuffer[String] = new ArrayBuffer[String]()
        val fjson = new FeatureJSON()
        geoObjectsAndDimensionKeys.foreach{ keys =>
          println(keys(0))
          val listString = keys(0)
          val geoObjectKeys = listString.substring(listString.indexOf("(")+1,listString.indexOf(")")).split(", ")

          val productKey = keys(1)
          val sql = "select phenomenon_time from gc_product where product_key=" + productKey + ";"
          val rs = statement.executeQuery(sql)
          //矢量的时间
          var time:Long ="20200101".toLong
          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          if (rs.first()) {
            rs.previous()
            while (rs.next()) {
              time = sdf.parse(rs.getString(1)).getTime
            }
          } else {
            println("No vector time acquired!")
          }
          //获取第一个矢量的瓦片元信息得到col和row
          val col_row =getVectorMeta("hbase_vector", geoObjectKeys(0), "vectorData", "tilesMetaData").dropRight(1).substring(1)
          val json = new JsonParser()
          val obj = json.parse(col_row).asInstanceOf[JsonObject]
          val extent = json.parse(obj.get("extent").toString).asInstanceOf[JsonObject]
          val (col, row) = (extent.get("column").toString.toInt, extent.get("row").toString.toInt)

          //          val geoJsons: ArrayBuffer[JsonObject] = new ArrayBuffer[JsonObject]()
          geoObjectKeys.foreach{geoObjectkey =>
            println(geoObjectkey)
            val metaJson = getVectorMeta("hbase_vector", geoObjectkey, "vectorData", "metaData")
//            ObjectMapper
            //            val featureStr = getVectorCell("hbase_vector", geoObjectkey, "vectorData", "metaData")
            val Json = json.parse(metaJson).asInstanceOf[JsonObject]
            //            geoJsons.append(Json)
//            VectorGeoJsonArray.append(Json)
            VectorGeoJsonArray.append(metaJson)
          }

        }
        //        println(GridLayerGeoObjectArray.toArray)
        println(VectorGeoJsonArray.toArray)
        bufferAsJavaList(VectorGeoJsonArray)
        //        VectorGeoJsonArray.toArray

      }finally
        conn.close()
    }else
      throw new RuntimeException("connection failed")

  }

}

