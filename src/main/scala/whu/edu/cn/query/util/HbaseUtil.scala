package whu.edu.cn.query.util

import java.io.IOException

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase._

import scala.collection.mutable.ArrayBuffer
import scala.io.StdIn

/**
 * @author czp
 * @date 2020/4/23 18:31
 * @version 1.0
 */
object HbaseUtil {
  System.setProperty("hadoop.home.dir", "/home/geocube/hadoop")
  //  var zookeeperQuorum = "192.168.56.101"
  var zookeeperQuorum = "125.220.153.26"
  val configuration = HBaseConfiguration.create()
//    configuration.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum)
  configuration.set(HConstants.ZOOKEEPER_QUORUM, "gisweb1:2181,gisweb3:2181,gisweb4:2181")
//    configuration.set(HConstants.ZOOKEEPER_QUORUM, "125.220.153.26,125.220.153.22,125.220.153.23")
//    configuration.set("hbase.master","gisweb1:60010");
  //提高RPC通信时长
  configuration.set("hbase.rpc.timeout", "200000")
  //设置Scan缓存
  configuration.set("hbase.client.scanner.caching", "200000")
  //
  configuration.set("hbase.client.scanner.timeout.period", "200000");

  configuration.setInt("mapreduce.task.timeout", 60000);
  //configuration.set ("zookeeper.znode.parent", "/hbase-unsecure") //看情况有时候要加有时候不加
  val connection = ConnectionFactory.createConnection(configuration)
//  println(connection)
  val admin = connection.getAdmin

  def main(args: Array[String]): Unit = {
    //    createTable("hbase_raster_regions",Array("tiles"))
    //    dropTable("hbase_raster_regions")
    //insertTable("1", "i", "age", "22")
//        scanDataFromHTable("hbase_raster_regions", "rasterData","metaData")
//        getRow("hbase_vector","Hainan_Daguangba_School_Vector_c4dde473-d024-4e54-a5b8-b2ddeb57ef5c")
    getTileMeta("hbase_raster_regions","000~0000000013","rasterData","metaData")
//    getTileCell("hbase_raster_regions","000~0000000013","rasterData","tile")
//    getVectorCell("hbase_vector","fx_xx_ffd4bce2-3cfe-4453-980f-bd5ab4a61db1","vectorData","tile")
//    getVectorMeta("hbase_vector","Hainan_Daguangba_ST_Vector_0dda2217-659d-4db4-b6ff-0c1c244896a6","vectorData","metaData")
//    getVectorTilesMeta("hbase_vector","fx_xx_ffd4bce2-3cfe-4453-980f-bd5ab4a61db1","vectorData","tilesMetaData")
    //    deleteRecord("1","i","name")
//    getRow("hbase_raster_regions","000~0000000013")
//    val rowlist = ArrayBuffer("000~0000000013","001~0000000014","002~0000000015","003~0000000016","004~0000000017","005~0000000018","006~0000000019","002~0000000020")
    val rowlist = ArrayBuffer("000~0000000013","001~0000000014")
    //    getTileCellsMap(rowlist,"hbase_raster_regions","rasterData","tile")
//    getTileData("hbase_raster_regions",rowlist)
    close()
    println("Hit enter to exit")
    StdIn.readLine()
  }
  //创建一个hbase表
  def createTable(tableName: String, columnFamilys: Array[String]) = {
    //操作的表名
    val tName = TableName.valueOf(tableName)
    //当表不存在的时候创建Hbase表
    if (!admin.tableExists(tName)) {
      //创建Hbase表模式
      val descriptor = new HTableDescriptor(tName)
      //创建列簇i
      for (columnFamily <- columnFamilys) {
        descriptor.addFamily(new HColumnDescriptor(columnFamily))
      }
      //创建表
      admin.createTable(descriptor)
      println("create successful!!")
    }
  }
  def scanDataFromHTable(tableName:String,columnFamily: String, column: String) = {
    val table = connection.getTable(TableName.valueOf(tableName))
    //定义scan对象
    val scan = new Scan()
    //添加列簇名称
    scan.addFamily(columnFamily.getBytes())
    //从table中抓取数据来scan
    val scanner = table.getScanner(scan)
    var result = scanner.next()
    //数据不为空时输出数据
    while (result != null) {
      //println(s"rowkey:${Bytes.toString(result.getRow)},列簇:${columnFamily}:${column},value:${Bytes.toString(result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column)))}")
      println(s"rowkey:${Bytes.toString(result.getRow)}")
      result = scanner.next()
    }
    //通过scan取完数据后，记得要关闭ResultScanner，否则RegionServer可能会出现问题(对应的Server资源无法释放)
    scanner.close()
  }
  def getTileData(tableName:String,rowkeyList: ArrayBuffer[String]):Map[String,(Array[Byte],String)]= {
    val table = connection.getTable(TableName.valueOf(tableName)) // 获取表
    var resultMap = Map[String, (Array[Byte], String)]()
    val getList = new ArrayBuffer[Get]()
    var cellIndex = 0
    var metaIndex = 0
    var cellMap = Map[String, Array[Byte]]()
    var metaMap = Map[String, String]()
    import scala.collection.JavaConversions._
    for (rowkey <- rowkeyList) { //把rowkey加到get里，再把get装到list中
      val get = new Get(Bytes.toBytes(rowkey))
      getList.add(get)
    }
    val results = table.get(getList) //重点在这，直接查getList<Get>
    for (result <- results) { //对返回的结果集进行操作
      for (rowKv <- result.rawCells()) {

        println("Family:" + new String(rowKv.getFamilyArray, rowKv.getFamilyOffset, rowKv.getFamilyLength))
        println("Qualifier:" + new String(rowKv.getQualifierArray, rowKv.getQualifierOffset, rowKv.getQualifierLength))
        println("TimeStamp:" + rowKv.getTimestamp)
        println("rowkey:" + new String(rowKv.getRowArray, rowKv.getRowOffset, rowKv.getRowLength))
        //      println("Value:" + new String(rowKv.getValueArray, rowKv.getValueOffset, rowKv.getValueLength, "UTF-8"))
        //      println("Value:" +Bytes.toInt(rowKv.getValueArray,3))
        val colName = Bytes.toString(rowKv.getQualifierArray, rowKv.getQualifierOffset, rowKv.getQualifierLength)
//        val value = Bytes.toString(rowKv.getValueArray, rowKv.getValueOffset,rowKv.getValueLength)
        println("colName:"+colName)
//        println(value)

        if(colName.equals("tile")){
          println("equalsTile")
//          var res= (rowKv.getValueArray, rowKv.getValueOffset,rowKv.getValueLength)
          var res= rowKv.getValueArray
          cellMap += (rowkeyList.get(cellIndex) -> res)
          cellIndex += 1
        }
        else if(colName.equals("metaData")){
          println("equalsMeta")
          var res= Bytes.toString(rowKv.getValueArray, rowKv.getValueOffset,rowKv.getValueLength)
          metaMap += (rowkeyList.get(metaIndex) -> res)
          metaIndex += 1
        }
      }
    }

//    println(metaMap.keys)
//    println(metaMap.values)
    for(key<-rowkeyList){
      resultMap+=(key->(cellMap(key),metaMap(key)))
    }

    return resultMap
  }
  //获取影像元信息
  def getTileMeta(tableName: String,rowKey:String,family:String,col:String):String={
    val table = connection.getTable(TableName.valueOf(tableName))
    val get: Get = new Get(Bytes.toBytes(rowKey))
    if(!get.isCheckExistenceOnly){
      get.addColumn(Bytes.toBytes(family),Bytes.toBytes(col))
      val result: Result = table.get(get)
      val res = Bytes.toString(result.getValue(Bytes.toBytes(family),Bytes.toBytes(col)))
      println(res)
      res
    }else{
      throw new RuntimeException("No data of rowkey = " + rowKey + " in HBase!")
    }
  }
  //获取影像瓦片list
  @throws[IOException]
  def getTileCellsMap(rowkeyList: ArrayBuffer[String],tableName: String,family:String,col:String): Map[String,Array[Byte]] = {
    val getList = new ArrayBuffer[Get]()
    var index = 0
    var resultMap = Map[String,Array[Byte]]()
    val table = connection.getTable(TableName.valueOf(tableName)) // 获取表
    import scala.collection.JavaConversions._
    for (rowkey <- rowkeyList) { //把rowkey加到get里，再把get装到list中
      val get = new Get(Bytes.toBytes(rowkey))
      getList.add(get)
    }
    val results = table.get(getList) //重点在这，直接查getList<Get>
    for (result <- results) { //对返回的结果集进行操作
      val res = result.getValue(Bytes.toBytes(family),Bytes.toBytes(col))
      resultMap += (rowkeyList.get(index)->res)
      index+=1
    }
//    println(resultMap.keys)
//    println(resultMap.values)
    resultMap
  }

  //获取影像元信息list
  def getTileMetasMap(rowkeyList: ArrayBuffer[String],tableName: String,family:String,col:String): Map[String,String] = {
    val getList = new ArrayBuffer[Get]()
    var index = 0
    var resultMap = Map[String,String]()
    val table = connection.getTable(TableName.valueOf(tableName)) // 获取表
    import scala.collection.JavaConversions._
    for (rowkey <- rowkeyList) { //把rowkey加到get里，再把get装到list中
      val get = new Get(Bytes.toBytes(rowkey))
      getList.add(get)
    }
    val results = table.get(getList) //重点在这，直接查getList<Get>
    for (result <- results) { //对返回的结果集进行操作
      val res =  Bytes.toString(result.getValue(Bytes.toBytes(family),Bytes.toBytes(col)))
      resultMap += (rowkeyList.get(index)->res)
      index+=1
    }
//    println(resultMap.keys)
//    println(resultMap.values)
    resultMap
  }
  //获取影像元信息
  def getTileCell(tableName: String,rowKey:String,family:String,col:String):Array[Byte]={
    val table = connection.getTable(TableName.valueOf(tableName))
    val get = new Get(Bytes.toBytes(rowKey))
    if(!get.isCheckExistenceOnly){
      get.addColumn(Bytes.toBytes(family),Bytes.toBytes(col))
      val result = table.get(get)
      val res = result.getValue(Bytes.toBytes(family),Bytes.toBytes(col))
//      println(res)
      res
    }else{
      throw new RuntimeException("No data of rowkey = " + rowKey + " in HBase!")
    }
  }
  //获取矢量元信息
  def getVectorMeta(tableName: String,rowKey:String,family:String,col:String):String={
    val table = connection.getTable(TableName.valueOf(tableName))
    val get: Get = new Get(Bytes.toBytes(rowKey))
    if(!get.isCheckExistenceOnly){
      get.addColumn(Bytes.toBytes(family),Bytes.toBytes(col))
      val result: Result = table.get(get)
      val rowKv = result.rawCells().last
      val res = new String(rowKv.getValueArray, rowKv.getValueOffset, rowKv.getValueLength, "UTF-8")

      //      val res = result.getValue(Bytes.toBytes(family),Bytes.toBytes(col))
      println(res)
      res
    }else{
      throw new RuntimeException("No data of rowkey = " + rowKey + " in HBase!")
    }
  }
  //获取矢量数据
  def getVectorCell(tableName: String,rowKey:String,family:String,col:String):String={
    val table = connection.getTable(TableName.valueOf(tableName))
    val get: Get = new Get(Bytes.toBytes(rowKey))
//    print(get)
    if(!get.isCheckExistenceOnly){
      get.addColumn(Bytes.toBytes(family),Bytes.toBytes(col))
      val result: Result = table.get(get)
      val rowKv = result.rawCells().last
      val res = new String(rowKv.getValueArray, rowKv.getValueOffset, rowKv.getValueLength, "UTF-8")
//      println(res)
      res
    }else{
      throw new RuntimeException("No data of rowkey = " + rowKey + " in HBase!")
    }
  }
  //获取矢量的瓦片元信息
  def getVectorTilesMeta(tableName: String,rowKey:String,family:String,col:String):String={
    val table = connection.getTable(TableName.valueOf(tableName))
    val get: Get = new Get(Bytes.toBytes(rowKey))
    if(!get.isCheckExistenceOnly){
      get.addColumn(Bytes.toBytes(family),Bytes.toBytes(col))
      val result: Result = table.get(get)
      val rowKv = result.rawCells().last
      val res = new String(rowKv.getValueArray, rowKv.getValueOffset, rowKv.getValueLength, "UTF-8")
      println(res)
      res
    }else{
      throw new RuntimeException("No data of rowkey = " + rowKey + " in HBase!")
    }
  }
  //获取数据
  def getRow(tableName:String, rowKey: String): Result = {
    val table = connection.getTable(TableName.valueOf(tableName))
    val get: Get = new Get(Bytes.toBytes(rowKey))
    val result: Result = table.get(get)
    for (rowKv <- result.rawCells()) {
//      println("Family")
//      println(rowKv.getFamilyArray)
//      println( new String(rowKv.getFamilyArray, rowKv.getFamilyOffset, rowKv.getFamilyLength))
//      println("Family:" + new String(rowKv.getFamilyArray, rowKv.getFamilyOffset, rowKv.getFamilyLength, "UTF-8"))
//      println("Qualifier:" + new String(rowKv.getQualifierArray, rowKv.getQualifierOffset, rowKv.getQualifierLength, "UTF-8"))
//      println("TimeStamp:" + rowKv.getTimestamp)
//      println("rowkey:" + new String(rowKv.getRowArray, rowKv.getRowOffset, rowKv.getRowLength, "UTF-8"))
      println("Family:" + new String(rowKv.getFamilyArray, rowKv.getFamilyOffset, rowKv.getFamilyLength))
      println("Qualifier:" + new String(rowKv.getQualifierArray, rowKv.getQualifierOffset, rowKv.getQualifierLength))
      println("TimeStamp:" + rowKv.getTimestamp)
      println("rowkey:" + new String(rowKv.getRowArray, rowKv.getRowOffset, rowKv.getRowLength))
//      println("Value:" + new String(rowKv.getValueArray, rowKv.getValueOffset, rowKv.getValueLength, "UTF-8"))
//      println("Value:" +Bytes.toInt(rowKv.getValueArray,3))
//      val colName = Bytes.toString(rowKv.getQualifierArray, rowKv.getQualifierOffset, rowKv.getQualifierLength)
//      val value = Bytes.toString(rowKv.getValueArray, rowKv.getValueOffset,rowKv.getValueLength)
//      println(colName)
//      println(value)
      val offset=rowKv.getValueOffset
      val arrbyte=rowKv.getValueArray
      //      ImageUtil.doSmth(arrbyte)
      //      val imUtil=new ImageUtil()
      //      imUtil.byteToImage(arrbyte)
      //      val valueLength=rowKv.getValueLength
      //      val subBytes=ImageUtil.subBytes(arrbyte,offset,valueLength)
      //      ImageUtil.doSmth(subBytes)
      //      val intArray=subBytes.map(_.toInt)
      //      System.out.println(util.Arrays.toString(intArray))
      //      val outp="output/00000000004.jpg"
      //      ImageUtil.writeImageFromArray(outp,"jpg",intArray,256,256)
      ////      ImageUtil.imageToGray(outp)
      //      ImageUtil.grayImage(4,outp,outp)
      //      ImageUtil.binaryImage(outp)
      //      ImageUtil.black(outp)
      //      val num=arrbyte(3)
      //      println("num",num)
      //      println("doubearray(3)",intArray(3))
      println("rowKv.getValueArray.length:" +rowKv.getValueArray.length)
      println("rowKv.getValueLength:" +rowKv.getValueLength)
      println("rowKv.getValueOffset:" +rowKv.getValueOffset)

    }
    return result
  }


  def insertData(/*conf: Configuration,*/ tableName: String, rowKey: String, columnFamily: String, column: String, value: Array[Byte]): Unit = {
    //创建连接
    //    val con = ConnectionFactory.createConnection(conf)
    //
    val table = connection.getTable(TableName.valueOf(tableName))
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), value)
    table.put(put)
    //    close(table, con)
    print("数据插入成功")
  }


  def dropTable(tableName: String):Unit={
    admin.disableTable(TableName.valueOf(tableName))
    admin.deleteTable(TableName.valueOf(tableName))
    println("drop successful!!")
  }
  // 关闭 connection 连接
  def close()={
    if (connection!=null){
      try{
        connection.close()
        println("关闭成功!")
      }catch{
        case e:IOException => println("关闭失败!")
      }
    }
  }
}
