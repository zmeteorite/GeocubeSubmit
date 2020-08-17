package whu.edu.cn.query.entity

import geotrellis.layer.TileLayerMetadata

import scala.collection.mutable.ArrayBuffer

case class RasterTileLayerMetadata[K](
_tileLayerMetadata: TileLayerMetadata[K],
_rasterProductName: String = "",
_rasterProductNames: ArrayBuffer[String] = new ArrayBuffer[String](),
_measurementNames: ArrayBuffer[String] = new ArrayBuffer[String]
) {
  var tileLayerMetadata: TileLayerMetadata[K] = _tileLayerMetadata
  var rasterProductName: String = _rasterProductName
  var rasterProductNames: ArrayBuffer[String] = _rasterProductNames
  var measurementNames: ArrayBuffer[String] = _measurementNames

  def getTileLayerMetadata: TileLayerMetadata[K] = this.tileLayerMetadata

  def getRasterProductName: String = this.rasterProductName

  def getMeasurementNames: ArrayBuffer[String] = this.rasterProductNames

  def getRasterProductNames: ArrayBuffer[String] = this.measurementNames

  def setTileLayerMetadata(tileLayerMetadata: TileLayerMetadata[K]) =
    this.tileLayerMetadata = tileLayerMetadata

  def setRasterProductName(rasterProductName: String) =
    this.rasterProductName = rasterProductName

  def setRasterProductNames(rasterProductNames: ArrayBuffer[String]) =
    this.rasterProductNames = rasterProductNames

  def setRasterProductNames(rasterProductNames: Array[String]) =
    this.rasterProductNames = rasterProductNames.toBuffer.asInstanceOf[ArrayBuffer[String]]

  def setMeasurementNames(measurementNames: ArrayBuffer[String]) =
    this.measurementNames = measurementNames

  def setMeasurementNames(measurementNames: Array[String]) =
    this.measurementNames = measurementNames.toBuffer.asInstanceOf[ArrayBuffer[String]]

}
