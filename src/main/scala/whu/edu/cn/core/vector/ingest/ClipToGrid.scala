package whu.edu.cn.core.vector.ingest

import whu.edu.cn.core.vector.grid.{GridConf, IndexFactory}
import whu.edu.cn.core.vector.rdd.{GeoObject, GeoObjectRDD, SpatialRDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom._
import geotrellis.vector.Extent

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.io.StdIn
import java.util.List

import geotrellis.layer.{LayoutDefinition, SpatialKey}
import geotrellis.raster.TileLayout
import geotrellis.store.index.{KeyIndex, ZCurveKeyIndexMethod}

object ClipToGrid {
  val inputPathWKT:String = "F:\\Scala\\IDAE_Env\\data\\pdt.txt"
  val inputPathShp:String = "E:\\VectorData\\Hainan_Daguangba\\sourceSchool\\fx_xx.shp"
  val inputPathGeojson:String = "E:\\VectorData\\Hainan_Daguangba\\sourceSchool\\fx_xx.geojson"

  val outputPathWKT:String = "F:\\Scala\\IDAE_Env\\data\\pdt2ZOrder"
  //val outputPath:String = "F:\\Scala\\IDAE_Env\\data\\pdt2Hilbert"
  val outputPathShp:String = "E:\\VectorData\\Hainan_Daguangba\\sourceSchool\\school_shp2ZOrder"
  val outputPathGeojson:String = "E:\\VectorData\\Hainan_Daguangba\\sourceSchool\\school_geojson2ZOrder"


  /**
   * Group geometry into grid with Hilbert coding.
   *
   * @param x Input geometry which is represented by pair(id, geometry)
   * @param indexFactory A predefined grid layout
   * @return Geometry and attched with intersected grid index, pair(HilbertIndex,(id, geometry))
   */
  def geom2Hilbert(x: (String, Geometry), indexFactory: IndexFactory): Array[(String, (String, Geometry))] = {
    val results: ArrayBuffer[(String, (String, Geometry))] = new ArrayBuffer[(String, (String, Geometry))]()
    x._2 match {
      case point: Point => {
        val gridIndex: String = indexFactory.createIndex(point)
        results.append((gridIndex, x))
      }
      case geom: Geometry => {
        val gridIndexs: List[String] = indexFactory.createIndexList(geom)
        for (gridIndex <- gridIndexs)
          results.append((gridIndex, x))
      }
    }
    results.toArray
  }

  /**
   * Group geometry into grid with custom xy coding.
   *
   * @param x Input geometry which is represented by pair(id, geometry)
   * @param gridConf A predefined grid layout
   * @return Geometry and attched with intersected grid index, pair(xyIndex,(id, geometry))
   */
  def geom2XY(x: (String, Geometry), gridConf: GridConf): Array[(Long, (String, Geometry))] = {
    val results: ArrayBuffer[(Long, (String, Geometry))] = new ArrayBuffer[(Long, (String, Geometry))]()
    val extent = gridConf.extent
    val gridDimX = gridConf.gridDimX
    val gridDimY = gridConf.gridDimY

    val mbr = (x._2.getEnvelopeInternal.getMinX, x._2.getEnvelopeInternal.getMaxX,
      x._2.getEnvelopeInternal.getMinY, x._2.getEnvelopeInternal.getMaxY)

    val gridSizeX = (extent.xmax - extent.xmin) / gridDimX.toDouble
    val gridSizeY = (extent.ymax - extent.ymin) / gridDimY.toDouble
    val _xmin: Long = (math.floor((mbr._1 - extent.xmin) / gridSizeX) max 0).toLong
    val _xmax: Long = (math.ceil((mbr._2 - extent.xmin) / gridSizeX) min gridDimX).toLong
    val _ymin: Long = (math.floor((mbr._3 - extent.ymin) / gridSizeY) max 0).toLong
    val _ymax: Long = (math.ceil((mbr._4 - extent.ymin) / gridSizeY) min gridDimY).toLong

    for (i <- _xmin to _xmax; j <- _ymin to _ymax) {
      val gridExtent = Extent(extent.xmin + i*gridSizeX, extent.ymin + j*gridSizeY,
        extent.xmin + (i + 1)*gridSizeX, extent.ymin + (j + 1)*gridSizeY)
      if(gridExtent.intersects(x._2)){
        val index = j * gridDimX.toLong + i
        val pair = (index, x)
        results.append(pair)
      }
    }

    results.toArray
  }

  /**
   * Group geometry into grid with ZOrder coding using SpatialRDD.
   *
   * @param x Input geometry which is represented by pair(id, geometry)
   * @param gridConf A predefined grid layout
   * @return Geometry and attched with intersected grid index, pair(ZOrderIndex,(id, geometry))
   */
  def geom2ZOrder(x: (String, Geometry), gridConf: GridConf): Array[(Long, (String, Geometry))] = {
    val results: ArrayBuffer[(Long, (String, Geometry))] = new ArrayBuffer[(Long, (String, Geometry))]()
    val extent = gridConf.extent
    val gridDimX = gridConf.gridDimX
    val gridDimY = gridConf.gridDimY
    val mbr = (x._2.getEnvelopeInternal.getMinX, x._2.getEnvelopeInternal.getMaxX,
      x._2.getEnvelopeInternal.getMinY, x._2.getEnvelopeInternal.getMaxY)

    val gridSizeX = (extent.xmax - extent.xmin) / gridDimX.toDouble
    val gridSizeY = (extent.ymax - extent.ymin) / gridDimY.toDouble
    val _xmin: Int = (math.floor((mbr._1 - extent.xmin) / gridSizeX) max 0).toInt
    val _xmax: Int = (math.ceil((mbr._2 - extent.xmin) / gridSizeX) min gridDimX).toInt
    val _ymin: Int = (math.floor((mbr._3 - extent.ymin) / gridSizeY) max 0).toInt
    val _ymax: Int = (math.ceil((mbr._4 - extent.ymin) / gridSizeY) min gridDimY).toInt

    val keyIndex: KeyIndex[SpatialKey] = ZCurveKeyIndexMethod.createIndex(null)

    val tl = TileLayout(360, 180, gridDimX.toInt, gridDimY.toInt) //tile:1°×1°, gridDimX.toInt × gridDimY.toInt
    val ld = LayoutDefinition(extent, tl)

    for (i <- _xmin to _xmax; j <- _ymin to _ymax) {
      val geotrellis_j = (gridDimY - 1 - j).toInt //geotrellis中网格xy编码为从左到右和从上至下，而此处的y为从下至上，因此转换
      val gridExtent = ld.mapTransform.keyToExtent(i, geotrellis_j)
      if (gridExtent.intersects(x._2)){
        val index = keyIndex.toIndex(SpatialKey(i, geotrellis_j))
        val pair = (index.toLong, x)
        results.append(pair)
      }
    }

    results.toArray
  }

  /**
   * Group geometry into grid with ZOrder coding using GeoObjectRDD.
   *
   * @param x Input geometry which is represented by pair(id, geometry)
   * @param gridConf A predefined grid layout
   * @return Geometry and attched with intersected grid index, pair(ZOrderIndex,(id, geometry))
   */
  def geom2ZOrder(x: GeoObject, gridConf: GridConf): Array[(Long, GeoObject)] = {
    val results: ArrayBuffer[(Long, GeoObject)] = new ArrayBuffer[(Long, GeoObject)]()
    val extent = gridConf.extent
    val gridDimX = gridConf.gridDimX
    val gridDimY = gridConf.gridDimY

    val feature = x.feature
    val geom = feature.getDefaultGeometry.asInstanceOf[Geometry]
    val mbr = (geom.getEnvelopeInternal.getMinX,geom.getEnvelopeInternal.getMaxX,
      geom.getEnvelopeInternal.getMinY, geom.getEnvelopeInternal.getMaxY)

    val gridSizeX = (extent.xmax - extent.xmin) / gridDimX.toDouble
    val gridSizeY = (extent.ymax - extent.ymin) / gridDimY.toDouble
    val _xmin: Int = (math.floor((mbr._1 - extent.xmin) / gridSizeX) max 0).toInt
    val _xmax: Int = (math.ceil((mbr._2 - extent.xmin) / gridSizeX) min gridDimX).toInt
    val _ymin: Int = (math.floor((mbr._3 - extent.ymin) / gridSizeY) max 0).toInt
    val _ymax: Int = (math.ceil((mbr._4 - extent.ymin) / gridSizeY) min gridDimY).toInt

    val keyIndex: KeyIndex[SpatialKey] = ZCurveKeyIndexMethod.createIndex(null)

    val tl = TileLayout(360, 180, gridDimX.toInt, gridDimY.toInt) //tile:1°×1°, gridDimX.toInt × gridDimY.toInt
    val ld = LayoutDefinition(extent, tl)

    for (i <- _xmin to _xmax; j <- _ymin to _ymax) {
      val geotrellis_j = (gridDimY - 1 - j).toInt //geotrellis中网格xy编码为从左到右从上至下，而此处的y为从下至上，因此转换
      val gridExtent = ld.mapTransform.keyToExtent(i, geotrellis_j)
      if (gridExtent.intersects(geom)){
        val index = keyIndex.toIndex(SpatialKey(i, geotrellis_j))
        val pair = (index.toLong, x)
        results.append(pair)
      }
    }

    results.toArray
  }

  /**
   * Group geometry into grid with Spatial Key  using SpatialRDD.
   *
   * @param x Input geometry which is represented by pair(id, geometry)
   * @param gridConf A predefined grid layout
   * @return Geometry and attched with intersected grid index, pair(SpatialKeyIndex,(id, geometry))
   */
  def geom2SpatialKey(x: (String, Geometry), gridConf: GridConf): Array[(SpatialKey, (String, Geometry))] = {
    val results: ArrayBuffer[(SpatialKey, (String, Geometry))] = new ArrayBuffer[(SpatialKey, (String, Geometry))]()
    val extent = gridConf.extent
    val gridDimX = gridConf.gridDimX
    val gridDimY = gridConf.gridDimY
    val mbr = (x._2.getEnvelopeInternal.getMinX, x._2.getEnvelopeInternal.getMaxX,
      x._2.getEnvelopeInternal.getMinY, x._2.getEnvelopeInternal.getMaxY)

    val gridSizeX = (extent.xmax - extent.xmin) / gridDimX.toDouble
    val gridSizeY = (extent.ymax - extent.ymin) / gridDimY.toDouble
    val _xmin: Int = (math.floor((mbr._1 - extent.xmin) / gridSizeX) max 0).toInt
    val _xmax: Int = (math.ceil((mbr._2 - extent.xmin) / gridSizeX) min gridDimX).toInt
    val _ymin: Int = (math.floor((mbr._3 - extent.ymin) / gridSizeY) max 0).toInt
    val _ymax: Int = (math.ceil((mbr._4 - extent.ymin) / gridSizeY) min gridDimY).toInt


    val tl = TileLayout(360, 180, gridDimX.toInt, gridDimY.toInt) //tile:1°×1°, gridDimX.toInt × gridDimY.toInt
    val ld = LayoutDefinition(extent, tl)

    for (i <- _xmin to _xmax; j <- _ymin to _ymax) {
      val geotrellis_j = (gridDimY - 1 - j).toInt //geotrellis中网格xy编码为从左到右从上至下，而此处的y为从下至上，因此转换
      val gridExtent = ld.mapTransform.keyToExtent(i, geotrellis_j)
      if (gridExtent.intersects(x._2)){
        val pair = (SpatialKey(i, geotrellis_j), x)
        results.append(pair)
      }
    }

    results.toArray
  }

  /**
   * Group geometry into grid with Spatial Key  using GeoObjectRDD.
   *
   * @param x Input geometry which is represented by pair(id, geometry)
   * @param gridConf A predefined grid layout
   * @return Geometry and attched with intersected grid index, pair(SpatialKeyIndex,(id, geometry))
   */
  def geom2SpatialKey(x: GeoObject, gridConf: GridConf): Array[(SpatialKey, GeoObject)] = {
    val results: ArrayBuffer[(SpatialKey, GeoObject)] = new ArrayBuffer[(SpatialKey, GeoObject)]()
    val extent = gridConf.extent
    val gridDimX = gridConf.gridDimX
    val gridDimY = gridConf.gridDimY
    val feature = x.feature
    val geom = feature.getDefaultGeometry.asInstanceOf[Geometry]
    val mbr = (geom.getEnvelopeInternal.getMinX,geom.getEnvelopeInternal.getMaxX,
      geom.getEnvelopeInternal.getMinY, geom.getEnvelopeInternal.getMaxY)

    val gridSizeX = (extent.xmax - extent.xmin) / gridDimX.toDouble
    val gridSizeY = (extent.ymax - extent.ymin) / gridDimY.toDouble
    val _xmin: Int = (math.floor((mbr._1 - extent.xmin) / gridSizeX) max 0).toInt
    val _xmax: Int = (math.ceil((mbr._2 - extent.xmin) / gridSizeX) min gridDimX).toInt
    val _ymin: Int = (math.floor((mbr._3 - extent.ymin) / gridSizeY) max 0).toInt
    val _ymax: Int = (math.ceil((mbr._4 - extent.ymin) / gridSizeY) min gridDimY).toInt


    val tl = TileLayout(360, 180, gridDimX.toInt, gridDimY.toInt) //tile:1°×1°, gridDimX.toInt × gridDimY.toInt
    val ld = LayoutDefinition(extent, tl)

    for (i <- _xmin to _xmax; j <- _ymin to _ymax) {
      val geotrellis_j = (gridDimY - 1 - j).toInt //geotrellis中网格xy编码为从左到右从上至下，而此处的y为从下至上，因此转换
      val gridExtent = ld.mapTransform.keyToExtent(i, geotrellis_j)
      if (gridExtent.intersects(geom)){
        val pair = (SpatialKey(i, geotrellis_j), x)
        results.append(pair)
      }
    }

    results.toArray
  }

  def apply(sc: SparkContext,
            spatialRdd: SpatialRDD,
            indexFactory: IndexFactory) = {
    var gridWithGeomIdList: RDD[(String, Iterable[(String, Geometry)])] = sc.emptyRDD
    gridWithGeomIdList = spatialRdd.flatMap(x => geom2Hilbert(x, indexFactory)).groupByKey()
    gridWithGeomIdList
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Vector Divide Using Spark")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)

    try {
      /***SpatialRDD***/
      /*val rdd:SpatialRDD = SpatialRDD.createSpatialRDDFromWKT(sc, inputPathWKT,8)
      //val rdd:SpatialRDD = SpatialRDD.createSpatialRDDFromShp(sc, inputPathShp,8)


      //Using Hilbert
      /*val indexFactory = new IndexFactory(-180.0, 180.0, -90.0, 90.0, 1.0, 1.0, 360*180)
      val gridWithGeomIdList = ClipToGrid(sc, rdd, indexFactory)*/

      //Using custom grid
      /*val gridConf: GridConf = new GridConf(360, 180, Extent(-180, -90, 180, 90))
      val gridWithGeomIdList = rdd.flatMap(x=>geom2XY(x, gridConf)).groupByKey()*/

      //Using ZOrder
      val gridConf: GridConf = new GridConf(360, 180, Extent(-180, -90, 180, 90))
      val gridWithGeomIdList = rdd.flatMap(x=>geom2ZOrder(x, gridConf)).groupByKey()

      gridWithGeomIdList.map(element=>{
        val results = new StringBuilder
        results.append(element._1+" : ")
        element._2.iterator.foreach(x=>results.append(x._1 + " "))
        results
      }).saveAsTextFile(outputPathShp)*/

      /***GeoObjectRDD***/
      //val rdd:GeoObjectRDD = GeoObjectRDD.createGeoObjectRDDFromShp(sc, inputPathShp,8)
      val rdd:GeoObjectRDD = GeoObjectRDD.createGeoObjectRDDFromGeojson(sc, inputPathGeojson,8)

      //using ZOrder
      val extent = Extent(-180, -90, 180, 90)
      val gridConf: GridConf = new GridConf(360, 180, Extent(-180, -90, 180, 90))
      val gridWithGeomIdList = rdd.flatMap(x=>geom2ZOrder(x, gridConf)).groupByKey()

      /*//using SpatialKey
      val gridConf: GridConf = new GridConf(360, 180, Extent(-180, -90, 180, 90))
      val gridWithGeomIdList = rdd.flatMap(x=>geom2SpatialKey(x, gridConf)).groupByKey()*/

      gridWithGeomIdList.map(element =>{
        val results = new StringBuilder
        results.append(element._1+" : ")
        element._2.iterator.foreach(x=>results.append(x.id + " "))
        results
      }).saveAsTextFile(outputPathGeojson)

      println("Hit enter to exit")
      StdIn.readLine()
    }
      finally
      {
        sc.stop()
      }
    }
}
