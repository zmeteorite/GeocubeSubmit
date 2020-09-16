package whu.edu.cn.mapper;


import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.context.annotation.Bean;
import whu.edu.cn.dao.NameDao;
import whu.edu.cn.dao.VectorDao;
import whu.edu.cn.entity.*;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.sql.Timestamp;
import java.util.List;
@Mapper
public interface ProductMapper extends BaseMapper<Product> {

    @Select("Select * from \"SensorLevelAndProduct\"")
    List<Product> getAllProducts();

    @Select("Select * from gc_product where product_type = \'Vector\'                                                                                          " )
    List<VectorDao> getVectorProducts();

    @Select("Select measurement_key,measurement_name,dtype from \"MeasurementsAndProduct\" where product_key = #{ProductKey}")
    List<Measurement> getMeasurements(Integer ProductKey);

    @Select("Select distinct measurement_name from \"MeasurementsAndProduct\" where product_name = #{ProductName}")
    List<MeasurementName> getMeasurementNamesByName(String ProductName);

    @Select("Select * from \"SensorLevelAndProduct\"where product_name = #{ProductName}" )
    List<Product> getEOProductsByName(String ProductName);

    @Select("Select * from gc_product where product_name = #{ProductName}" )
    List<VectorDao> getVectorProductsByName(String ProductName);


    @Select("Select distinct product_name,product_type from gc_product")
    List<NameDao> getAllProductNames();

    List<Product> getProductsByParams(@Param("productName") String ProductName, @Param("startTime") Timestamp StartTime, @Param("endTime") Timestamp EndTime, @Param("WKT") String WKT);




}
