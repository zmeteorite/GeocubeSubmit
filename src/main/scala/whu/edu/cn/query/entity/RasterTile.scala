package whu.edu.cn.query.entity

import geotrellis.raster.Tile

case class RasterTile(_ID: String = "", _productID: String = ""){
  var ID: String = _ID
  var productID: String = _productID
  var rowNum: String = ""
  var colNum: String = ""
  var leftBottomLat: String = ""
  var leftBottomLong: String = ""
  var rightUpperLat: String = ""
  var rightUpperLong: String = ""
  var productMeta: Product = Product()
  var extentMeta: Extent = Extent()
  var tileQualityMeta: TileQuality = TileQuality()
  var CRS: String = ""
  var measurement: Measurement = Measurement()
  var data:Tile = null

  def setData(_data: Tile): Unit = {
    data = _data
  }

  def setProductMeta(_productMeta: Product): Unit = {
    productMeta = _productMeta
  }

  def setMeasurement(_measurement: Measurement): Unit = {
    measurement = _measurement
  }

  def setExtentMeta(_extentMeta: Extent): Unit = {
    extentMeta = _extentMeta
  }

  def setColNum(_colNum: String): Unit = {
    colNum = _colNum
  }

  def setRowNum(_rowNum: String): Unit = {
    rowNum = _rowNum
  }

  def setCRS(_CRS: String): Unit = {
    CRS = _CRS
  }

  def setLeftBottomLat(_leftBottomLat: String): Unit = {
    leftBottomLat = _leftBottomLat
  }

  def setLeftBottomLong(_leftBottomLong: String): Unit = {
    leftBottomLong = _leftBottomLong
  }

  def setRightUpperLat(_rightUpperLat: String): Unit = {
    rightUpperLat = _rightUpperLat
  }

  def setRightUpperLong(_rightUpperLong: String): Unit = {
    rightUpperLong = _rightUpperLong
  }

  def setTileQualityMeta(_tileQualityMeta: TileQuality): Unit = {
    tileQualityMeta = _tileQualityMeta
  }

  def getColNum: String = colNum

  def getLeftBottomLat: String = leftBottomLat

  def getLeftBottomLong: String = leftBottomLong

  def getProductMeta: Product = productMeta

  def getRightUpperLat: String = rightUpperLat

  def getRightUpperLong: String = rightUpperLong

  def getExtentMeta: Extent = extentMeta

  def getRowNum: String = rowNum

  def getCRS: String = CRS

  def getData: Tile = data

  def getMeasurement: Measurement = measurement
}
