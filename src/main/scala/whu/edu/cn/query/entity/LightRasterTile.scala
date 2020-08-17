package whu.edu.cn.query.entity

import geotrellis.raster.Tile

case class LightRasterTile(_measurementName: String, _tile:Tile){
  var measurementName = _measurementName
  var tile = _tile

  def setMeasurementName(measurementName: String): Unit = {
    this.measurementName = measurementName
  }

  def getMeasurementName: String = measurementName

  def setTile(tile: Tile): Unit = {
    this.tile = tile
  }

  def getTile: Tile = this.tile
}
