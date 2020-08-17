package whu.edu.cn.query.entity

import java.sql.{DriverManager, ResultSet}

case class Measurement (_measurementID: String = "", _measurementName: String = "", _measurementDType: String = ""){
  var measurementID: String = _measurementID
  var measurementName: String = _measurementName
  var measurementDType: String = _measurementDType

  def this(measurementName: String){
    this()
    this.measurementName = measurementName
  }

  def setMeasurementName(_measurementName: String): Unit = {
    measurementName = _measurementName
  }

  def getMeasurementName: String = measurementName

  def setMeasurementDType(_measurementDType: String): Unit = {
    measurementDType = _measurementDType
  }

  def setMeasurementID(_measurementID: String): Unit = {
    measurementID = _measurementID
  }

  def getMeasurementDType: String = measurementDType

}

object Measurement{
  def getMeasurementMetaByMeaAndProKey(measurementKey: String, productKey: String, connAddr: String, user: String, password: String): Measurement = {
    val conn = DriverManager.getConnection(connAddr, user, password)
    if (conn != null) {
      try {
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        val sql = "select measurement_key, measurement_name, dtype " +
          "from \"MeasurementsAndProduct\" where product_key=" + productKey + " And measurement_key=" + measurementKey + ";"
        val rs = statement.executeQuery(sql)
        val measurement = new Measurement()
        //each tile has unique measurement
        while (rs.next) {
          measurement.setMeasurementID(rs.getString(1))
          measurement.setMeasurementName(rs.getString(2))
          measurement.setMeasurementDType(rs.getString(3))
        }
        /*println("Measurement meta of the queried MeaAndProKey:")
        println("measurementID/Key:" + measurement.measurementID,
          "measurementName:" + measurement.measurementName,
          "measurementDType:" + measurement.measurementDType)*/

        measurement
      } finally {
        conn.close
      }
    } else
      throw new RuntimeException("Null connection!")
  }

}
