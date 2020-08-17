package whu.edu.cn.core.vector.rdd

import java.util.UUID

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.geotools.data.shapefile.files.ShpFiles
import org.geotools.data.shapefile.shp.ShapefileReader
import org.locationtech.jts.geom._
import org.locationtech.jts.io.WKTReader

import scala.collection.mutable.ArrayBuffer

class SpatialRDD (val rddPrev: RDD[(String, Geometry)])extends RDD[(String, Geometry)](rddPrev){
  override val partitioner: Option[Partitioner] = rddPrev.partitioner
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[(String, Geometry)] = {
    rddPrev.iterator(split, context)
  }

  override protected def getPartitions: Array[Partition] = rddPrev.partitions

  override protected def getPreferredLocations(s: Partition): Seq[String] = rddPrev.preferredLocations(s)
}
object SpatialRDD {
  val COMMA = ","
  val TAB = "\t"
  val SPACE = " "
  var SEPARATOR = "\t"

  /**
   * 读取本地文件系统上的shapefile文件生成SpatialRDD，需设置partition数量
   *
   * @param sc Spark上下文
   * @param filePath     文件路径
   * @param numPartition 分区数
   * @return
   */
  def createSpatialRDDFromShp(implicit sc: SparkContext,
                              filePath: String,
                              numPartition: Int): SpatialRDD = {
    // 读取本地文件系统shp数据
    val geomArray: ArrayBuffer[(String, Geometry)] = new ArrayBuffer[(String, Geometry)]()
    val sf = new ShpFiles(filePath)
    val sfReader = new ShapefileReader(sf, false, false, new GeometryFactory())
    while(sfReader.hasNext){
      val uuid = UUID.randomUUID().toString
      val pair = (uuid, (sfReader.nextRecord().shape()).asInstanceOf[Geometry])
      geomArray += pair
    }
    val rddShpData = sc.parallelize(geomArray, numPartition)
    // 生成SpatialRDD返回
    new SpatialRDD(rddShpData)
  }

  /**
   * 读取本地文件系统或HDFS上wkt文件生成SpatialRDD，需设置partition数量
   *
   * @param sc Spark上下文
   * @param filePath     HDFS文件路径
   * @param numPartition 分区数
   * @return
   */
  def createSpatialRDDFromWKT(sc: SparkContext,
                               filePath: String,
                               numPartition: Int): SpatialRDD = {

    // 读取HDFS上txt数据
    var rddHDFSData = sc.textFile(filePath).filter(line => {
      val wkt = line.split(SEPARATOR)(0)
      if (wkt.contains("POLYGON") || wkt.contains("LINESTRING") || wkt.contains("POINT")) {
        val geom = new WKTReader().read(wkt)
        if (geom != null)
          if(geom.isValid())
            true
          else
            false
        else
          false
      } else {
        false
      }
    }).map(line => {
      val uuid = UUID.randomUUID().toString
      (uuid, line)
    })

    // 重分区
    rddHDFSData = rddHDFSData.repartition(numPartition)

    val rddSpatialData = rddHDFSData.map(lineWithUUID => {
      val wkt = lineWithUUID._2.split(SEPARATOR)(0)
      val geom = new WKTReader().read(wkt)
      (lineWithUUID._1, geom)

    })

    // 生成SpatialRDD返回
    new SpatialRDD(rddSpatialData)
  }



}
