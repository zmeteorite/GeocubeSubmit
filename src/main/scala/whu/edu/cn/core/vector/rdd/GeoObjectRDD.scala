package whu.edu.cn.core.vector.rdd

import java.io.{File, FileInputStream}
import java.nio.charset.Charset
import java.util.UUID
import scala.collection.mutable.ArrayBuffer
import scala.io.StdIn

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ArrayNode

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.geotools.data.shapefile.{ShapefileDataStore, ShapefileDataStoreFactory}
import org.geotools.data.simple.SimpleFeatureIterator
import org.geotools.geojson.feature.FeatureJSON
import org.opengis.feature.simple.SimpleFeature
import org.locationtech.jts.geom._


/**
 * RDD class which represents a series of GeoObjects
 *
 */
class GeoObjectRDD  (val rddPrev: RDD[GeoObject])extends RDD[GeoObject](rddPrev){
  override val partitioner: Option[Partitioner] = rddPrev.partitioner
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[GeoObject] = {
    rddPrev.iterator(split, context)
  }

  override protected def getPartitions: Array[Partition] = rddPrev.partitions

  override protected def getPreferredLocations(s: Partition): Seq[String] = rddPrev.preferredLocations(s)
}
object GeoObjectRDD{
  val COMMA = ","
  val TAB = "\t"
  val SPACE = " "
  var SEPARATOR = "\t"

  /**
   * Generate GeoObjectRDD using shapefile on local file system
   *
   * @param sc A Spark Context
   * @param filePath Shapefile path
   * @param numPartition Num of RDD partitions
   *
   * @return A GeoObjectRDD[GeoObject]
   */
  def createGeoObjectRDDFromShp(implicit sc: SparkContext,
                                filePath: String,
                                numPartition: Int): GeoObjectRDD = {
    val geomArray: ArrayBuffer[GeoObject] = new ArrayBuffer[GeoObject]()
    val dataStoreFactory = new ShapefileDataStoreFactory()
    val sds = dataStoreFactory.createDataStore(new File(filePath).toURI.toURL)
      .asInstanceOf[ShapefileDataStore]
    sds.setCharset(Charset.forName("GBK"))
    val featureSource = sds.getFeatureSource()

    val iterator: SimpleFeatureIterator = featureSource.getFeatures().features()
    while (iterator.hasNext) {
      val feature = iterator.next()
      val uuid = UUID.randomUUID().toString
      val geoObject = new GeoObject(uuid, feature)
      geomArray += geoObject
    }
    iterator.close()

    val rddShpData = sc.parallelize(geomArray, numPartition)
    new GeoObjectRDD(rddShpData)
  }

  /**
   * Generate GeoObjectRDD using Geojson on local file system
   *
   * @param sc A Spark Context
   * @param filePath Geojson path
   * @param numPartition Num of RDD partitions
   *
   * @return A GeoObjectRDD[GeoObject]
   */
  def createGeoObjectRDDFromGeojson(implicit sc: SparkContext,
                                    filePath: String,
                                    numPartition: Int): GeoObjectRDD = {
    val geomArray: ArrayBuffer[GeoObject] = new ArrayBuffer[GeoObject]()
    val objectMapper=new ObjectMapper()
    val fjson = new FeatureJSON()
    val node = objectMapper.readTree(new FileInputStream(filePath))
    if(node != null && node.has("features")) {
      val featureNodes = node.get("features").asInstanceOf[ArrayNode]
      val features = featureNodes.elements()
      while(features.hasNext){
        val arrayNode:JsonNode = features.next()
        val fcWkt = arrayNode.toString()
        //println(fcWkt)
        val feature:SimpleFeature = fjson.readFeature(fcWkt)
        val uuid = UUID.randomUUID().toString
        val geoObject = new GeoObject(uuid, feature)
        geomArray += geoObject
      }
    }
    val rddShpData = sc.parallelize(geomArray, numPartition)
    new GeoObjectRDD(rddShpData)
  }

  def main(args: Array[String]):Unit = {
    val inputPathShp:String = "E:\\VectorData\\Hainan_Daguangba\\sourceSchool\\fx_xx.shp"
    val inputPathGeojson:String = "E:\\VectorData\\Hainan_Daguangba\\sourceSchool\\fx_xx.geojson"

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("GeoObjectRDD")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)
    try {
      //val rdd:GeoObjectRDD = GeoObjectRDD.createGeoObjectRDDFromShp(sc, inputPathShp,8)
      val rdd:GeoObjectRDD = GeoObjectRDD.createGeoObjectRDDFromGeojson(sc, inputPathGeojson,8)
      rdd.collect().foreach(x=>{
        val id:String = x.id
        val feature:SimpleFeature = x.feature
        val geom:Geometry = feature.getDefaultGeometry.asInstanceOf[Geometry]
        val properties = feature.getProperties.iterator()
        while(properties.hasNext){
          val property = properties.next()
          val propertyValueType = property.getType.getBinding
          print(property.getName, propertyValueType.getName, property.getValue)
        }
        println()
      })

      println("Hit enter to exit")
      StdIn.readLine()
    }
    finally
    {
      sc.stop()
    }
  }

}

