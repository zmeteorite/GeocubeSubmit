package whu.edu.cn.query.util

import java.nio.ByteBuffer

import geotrellis.raster.render.{ColorRamp, RGB}
import geotrellis.raster.{FloatArrayTile, Tile, UByteArrayTile, UShortArrayTile}

import scala.collection.mutable.ArrayBuffer

object TileSerializer{
  def deserializeTileData(platform:String, tileBytes:Array[Byte], tileSize:Int, dataType:String): Tile = {
    platform match {
      case "GF1" =>{
        dataType match {
          case "uint16raw" => deserialize2UShortType(tileBytes, tileSize, dataType)
          case _ => throw new RuntimeException("No support for " + dataType)
        }
      }
      case "Landsat8" => {
        dataType match {
          case "uint8raw" => deserialize2UByteType(tileBytes, tileSize, dataType)
          case "float32" => deserialize2FloatType(tileBytes, tileSize, dataType)
          case _ => throw new RuntimeException("No support for " + dataType)
        }
      }
      case _ => throw new RuntimeException("No support for " + platform)
    }
  }

  //uint8raw, uint8
  def deserialize2UByteType(tileBytes: Array[Byte], tileSize: Int, dataType: String): Tile = {
    val index = ArrayBuffer.range(0, tileSize * tileSize)
    val cell = new Array[Byte](tileSize * tileSize)
    for (i <- index) {
      val h1 = Integer.toOctalString(tileBytes(i) & 0xFF)
      val l1 = Integer.parseInt(h1, 8)
      val str1 = Integer.toBinaryString(l1)
      val k = Integer.parseInt(str1, 2)
      if (k != 0) {
        cell(i) = k.toByte
      } else {
        cell(i) = 0
      }
    }
    UByteArrayTile(cell, tileSize, tileSize)
  }

  //uint16raw, uint16
  def deserialize2UShortType(tileBytes: Array[Byte], tileSize: Int, dataType: String): Tile = {
    val odd = ArrayBuffer.range(0, tileSize * tileSize * 2, 2)
    val even = ArrayBuffer.range(1, tileSize * tileSize * 2 + 1, 2)
    val index = ArrayBuffer.range(0, tileSize * tileSize)
    val cell = new Array[Short](tileSize * tileSize)
    for (i <- index) {
      val h1 = Integer.toHexString(tileBytes(odd(i)) & 0xFF)
      val l1 = Integer.parseInt(h1, 16)
      val str1 = Integer.toBinaryString(l1)
      val h2 = Integer.toHexString(tileBytes(even(i)) & 0xFF)
      val l2 = Integer.parseInt(h2, 16)
      val str2 = Integer.toBinaryString(l2)
      val k = Integer.parseInt(str1 + str2, 2)
      if (k != 0) {
        cell(i) = k.toShort
        cell(i) = 0
      }
    }
    UShortArrayTile(cell, tileSize, tileSize)
  }

  //float32raw, float32
  def deserialize2FloatType(tileBytes: Array[Byte], tileSize: Int, dataType: String): Tile = {
    val subFirst = ArrayBuffer.range(0, tileSize * tileSize * 4, 4) //float32=4*8,4194304=1024*1024*4
    val subSecond = ArrayBuffer.range(1, tileSize * tileSize * 4 + 1, 4)
    val subThird = ArrayBuffer.range(2, tileSize * tileSize * 4 + 2, 4)
    val subFourth = ArrayBuffer.range(3, tileSize * tileSize * 4 + 3, 4)
    val index = ArrayBuffer.range(0, tileSize * tileSize)
    val cell = new Array[Float](tileSize * tileSize)
    for (i <- index) {
      val _array: Array[Byte] = new Array[Byte](4)
      _array(0) = tileBytes(subFirst(i))
      _array(1) = tileBytes(subSecond(i))
      _array(2) = tileBytes(subThird(i))
      _array(3) = tileBytes(subFourth(i))
      cell(i) = ByteBuffer.wrap(_array).getFloat
    }
    FloatArrayTile(cell, tileSize, tileSize)
  }

  def tile2PngBytes(tile: Tile):Array[Byte] = {
    val colorRamp = ColorRamp(RGB(0,0,0), RGB(255,255,255))
      .stops(100)
      .setAlphaGradient(0xFF, 0xAA)
    val png = tile.renderPng(colorRamp)  //Array[Byte]
    png
  }

}
