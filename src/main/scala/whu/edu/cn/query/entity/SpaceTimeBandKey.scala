package whu.edu.cn.query.entity

import geotrellis.layer.SpaceTimeKey

case class SpaceTimeBandKey (_spaceTimeKey: SpaceTimeKey, _measurementName: String){
  val spaceTimeKey = _spaceTimeKey
  val measurementName = _measurementName
}
