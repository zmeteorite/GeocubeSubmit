package whu.edu.cn.core.vector.rdd

import java.io._
import java.nio.charset.Charset
import java.util
import java.util.UUID

import com.fasterxml.jackson.core.{JsonEncoding, JsonFactory}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ArrayNode
import org.geotools.data.Transaction
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.shapefile._
import org.geotools.data.simple._
import org.geotools.geojson.feature.FeatureJSON
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
 * Represent feature with id, feature contains geometry and its properties
 *
 */
class GeoObject(_id: String,
                _feature: SimpleFeature) extends Serializable {
  val id: String = _id
  val feature: SimpleFeature = _feature
}

object GeoObject{
  /****Test****/
  val inputPathShp:String = "E:\\VectorData\\Hainan_Daguangba\\sourceSchool\\fx_xx.shp"
  val inputPathGeojson:String = "E:\\VectorData\\Hainan_Daguangba\\sourceSchool\\fx_xx.geojson"
  val inputPathSingleGeojson:String = "E:\\VectorData\\Hainan_Daguangba\\sourceSchool\\single_feature.geojson"

  val outputPathShp:String = "E:\\VectorData\\Hainan_Daguangba\\affectedSchool\\affected_school.shp"
  val outputPathGeojson:String = "E:\\VectorData\\Hainan_Daguangba\\affectedSchool\\affected_school.geojson"

  def readShp(): Unit = {
    val dataStoreFactory = new ShapefileDataStoreFactory()
    try{
      val sds = dataStoreFactory.createDataStore(new File(inputPathShp).toURI.toURL)
        .asInstanceOf[ShapefileDataStore]
      sds.setCharset(Charset.forName("GBK"))
      val featureSource = sds.getFeatureSource()
      val iterator:SimpleFeatureIterator = featureSource.getFeatures().features()
      while(iterator.hasNext){
        val feature = iterator.next()
        val uuid = UUID.randomUUID().toString
        val geoObject = new GeoObject(uuid, feature)

        val geom = geoObject.feature.getDefaultGeometry.asInstanceOf[Geometry]
        val properties = geoObject.feature.getProperties.iterator()
        while(properties.hasNext){
          val property = properties.next()
          val propertyValueType = property.getType.getBinding
          print(property.getName, propertyValueType.getName, property.getValue)
          println(property)
        }
      }
      iterator.close()
    } catch {
      case ex: Exception =>{
        println(ex.printStackTrace())
      }
    }
  }

  def readGeojson(): Unit = {
    /*//Geometry
    val gjson = new GeometryJSON()
    val json:String = "{\"type\":\"Point\",\"coordinates\":[100.1,0.1]}"
    val reader = new StringReader(json)
    val p = gjson.read(reader)
    println(p)*/

    /*//Feature,数据库中存储的geojson文件，每条记录存储一个Feature
    val objectMapper=new ObjectMapper()
    val fjson = new FeatureJSON()
    val node = objectMapper.readTree(new FileInputStream(inputPathSingleGeojson))
    val fcWkt=node.toString()
    val feature = fjson.readFeature(fcWkt)
    val geom = feature.getDefaultGeometry.asInstanceOf[Geometry]
    val properties = feature.getProperties.iterator()
    while(properties.hasNext){
      val property = properties.next()
      val propertyValueType = property.getType.getBinding
      print(property.getName, propertyValueType.getName, property.getValue)
      println()
    }*/

    //FeatureCollection
    val objectMapper=new ObjectMapper()
    val fjson = new FeatureJSON()
    val node = objectMapper.readTree(new FileInputStream(inputPathGeojson))
    if(node != null && node.has("features")) {
      val featureNodes = node.get("features").asInstanceOf[ArrayNode]
      val features = featureNodes.elements()
      var countFeature = 0
      while(features.hasNext){
        countFeature += 1
        val arrayNode:JsonNode = features.next()
        //val featureNode = arrayNode.elements()
        val fcWkt=arrayNode.toString()
        val feature = fjson.readFeature(fcWkt)
        val geom = feature.getDefaultGeometry.asInstanceOf[Geometry]
        val properties = feature.getProperties.iterator()
        while(properties.hasNext){
          val property = properties.next()
          val propertyValueType = property.getType.getBinding
          print(property.getName, propertyValueType.getName, property.getValue)
          println()
        }
      }
      println(countFeature)
    }
  }

  def getFeatures():util.ArrayList[SimpleFeature] = {
    val featureList = new util.ArrayList[SimpleFeature]()
    val objectMapper=new ObjectMapper()
    val fjson = new FeatureJSON()
    val node = objectMapper.readTree(new FileInputStream(inputPathGeojson))
    if(node != null && node.has("features")) {
      val featureNodes = node.get("features").asInstanceOf[ArrayNode]
      val features = featureNodes.elements()
      var countFeature = 0
      while(features.hasNext && countFeature < 2){
        countFeature += 1
        val arrayNode:JsonNode = features.next()
        //val featureNode = arrayNode.elements()
        val fcWkt=arrayNode.toString()
        val feature = fjson.readFeature(fcWkt)
        featureList.add(feature)
        val geom = feature.getDefaultGeometry.asInstanceOf[Geometry]
        val properties = feature.getProperties.iterator()
        while(properties.hasNext){
          val property = properties.next()
          val propertyValueType = property.getType.getBinding
          //print(property.getName, propertyValueType.getName, property.getValue)
          //println()
        }
      }
      println(countFeature)
    }
    featureList
  }

  def writeGeojsonUsingGeotools(): Unit ={
    val features = getFeatures()
    val fjson = new FeatureJSON()
    val simpleFeatureType: SimpleFeatureType = features.get(0).getFeatureType
    val featureCollection:SimpleFeatureCollection = new ListFeatureCollection(simpleFeatureType, features)
    val fileOutputStream = new FileOutputStream(new File(outputPathGeojson))
    fjson.writeCRS(DefaultGeographicCRS.WGS84, fileOutputStream)
    fjson.writeFeatureCollection(featureCollection, fileOutputStream)

    fileOutputStream.close()
  }

  def writeGeojsonUsingJackson(): Unit ={
    val features = getFeatures()
    val outFile = new File(outputPathGeojson)
    val fjson = new FeatureJSON()
    val writer = new StringWriter()

    val jsonFactory = new JsonFactory()
    val jsonGenerator
    = jsonFactory.createGenerator(outFile, JsonEncoding.UTF8)
    jsonGenerator.writeStartObject()
    jsonGenerator.writeStringField("type", "FeatureCollection")
    jsonGenerator.writeStringField("name", outFile.getName.split(".")(0))
    jsonGenerator.writeArrayFieldStart("features")
    val it = features.iterator()
    while(it.hasNext){
      val feature = it.next()
      fjson.writeFeature(feature, writer)
      jsonGenerator.writeString(writer.toString)
    }
    jsonGenerator.writeEndArray()
    jsonGenerator.writeEndObject()
    jsonGenerator.close()
  }
  def writeShp() = {
    val features = getFeatures()

    val params = new util.HashMap[String, Serializable]()
    params.put(ShapefileDataStoreFactory.URLP.key, new File(outputPathShp).toURI().toURL())

    val factory = new ShapefileDataStoreFactory()
    val ds = factory.createNewDataStore(params).asInstanceOf[ShapefileDataStore]
    ds.createSchema(SimpleFeatureTypeBuilder.retype(features.get(0).getFeatureType, DefaultGeographicCRS.WGS84))
    ds.setCharset(Charset.forName("UTF-8"))
    val it = features.iterator()
    val writer = ds.getFeatureWriter(ds.getTypeNames()(0), Transaction.AUTO_COMMIT)

    while(it.hasNext){
      val f = it.next()
      val fNew = writer.next()
      fNew.setAttribute("the_geom", f.getDefaultGeometry.asInstanceOf[Geometry])
      val fProperties = f.getProperties.iterator()
      val fNewProperties = fNew.getProperties.iterator()
      fNewProperties.next()
      while(fProperties.hasNext && fNewProperties.hasNext){
        fNewProperties.next()
        val property = fProperties.next()
        fNew.setAttribute(property.getName, property.getValue)
      }
      println(f.toString)
      println(fNew.toString)
      writer.write()
    }
    writer.close()
    ds.dispose()

  }
  def main(args: Array[String]): Unit = {
    writeGeojsonUsingGeotools()
  }


}
