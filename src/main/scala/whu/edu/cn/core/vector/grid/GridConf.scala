package whu.edu.cn.core.vector.grid

import geotrellis.vector.Extent


class GridConf(_gridDimX: Long, _gridDimY: Long, _extent: Extent) extends Serializable {
  val gridDimX = _gridDimX
  val gridDimY = _gridDimY
  val extent = _extent
  val gridSizeX = (extent.xmax - extent.xmin) / gridDimX.toDouble
  val gridSizeY = (extent.ymax - extent.ymin) / gridDimY.toDouble
}
