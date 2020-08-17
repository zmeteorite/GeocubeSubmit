package whu.edu.cn.query.entity

import java.sql.{DriverManager, ResultSet}

case class TileQuality (_ID:String = "", _cloud:String = "", _cloudShadow:String = ""){
  var ID: String = _ID
  var cloud: String = _cloud
  var cloudShadow: String = _cloudShadow
}

object TileQuality{
  def getTileQualityMetaByKey(tileQualityKey:String, connAddr: String, user: String, password: String):TileQuality={
    val conn = DriverManager.getConnection(connAddr, user, password)
    if(conn!=null){
      try {
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        val sql = "select tile_quality_key,cloud,cloudshadow "+
          "from gc_tile_quality where tile_quality_key=" + tileQualityKey + ";"
        val rs = statement.executeQuery(sql)
        val rsArray = new Array[String](3)
        val columnCount = rs.getMetaData().getColumnCount()

        //each tile has unique extent object
        while (rs.next) {
          for (i <- 1 to columnCount)
            rsArray(i-1)=rs.getString(i)
        }

        //println("TileQuality meta of the queried tileQualityKey:")
        /* println("tileQualityID/Key:" + rsArray(0),
           "cloud:" + rsArray(1),
           "cloudShadow:" + rsArray(2))*/

        val quality = new TileQuality(rsArray(0),rsArray(1),rsArray(2))
        quality
      } finally {
        conn.close
      }
    } else
      throw new RuntimeException("Null connection!")
  }
}


