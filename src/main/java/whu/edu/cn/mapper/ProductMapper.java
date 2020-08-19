package whu.edu.cn.mapper;


import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import whu.edu.cn.entity.*;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.sql.Timestamp;
import java.util.List;
@Mapper
public interface ProductMapper extends BaseMapper<Product> {

    @Select("Select * from \"SensorLevelAndProduct\"")
    List<Product> getAllProducts();

    @Select("Select measurement_key,measurement_name,dtype from \"MeasurementsAndProduct\" where product_key = #{ProductKey}")
    List<Measurement> getMeasurements(Integer ProductKey);

    @Select("Select distinct measurement_name from \"MeasurementsAndProduct\" where product_name = #{ProductName}")
    List<MeasurementName> getMeasurementNamesByName(String ProductName);

    @Select("Select * from \"SensorLevelAndProduct\"where product_key = #{ProductKey}" )
    Product getProductById(Integer ProductKey);

    @Select("Select * from \"SensorLevelAndProduct\"where product_name = #{ProductName}" )
    List<Product> getProductsByName(String ProductName);

    @Select("Select * from gc_extent where extent_key = #{ExtentKey}")
    Extent getExtentByID(Integer ExtentKey);

    @Select("Select * from gc_tile_quality where tile_quality_key = #{TileQualityKey}")
    TileQuality getTileQualityByID(Integer TileQualityKey);

    @Select("Select * from gc_measurement where measurement_key = #{MeasurementKey}")
    Measurement getMeasurementByID(Integer MeasurementKey);

//    @Select("Select distinct product_name from \"SensorLevelAndProduct\"")
    @Select("Select distinct product_name from gc_product")
    List<String> getAllProductNames();

    List<Product> getProductsByParams(@Param("productName") String ProductName, @Param("startTime") Timestamp StartTime, @Param("endTime") Timestamp EndTime, @Param("WKT") String WKT);

    List<Product> selectProduct();


}
