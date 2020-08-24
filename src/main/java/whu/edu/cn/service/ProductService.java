package whu.edu.cn.service;

import com.baomidou.dynamic.datasource.annotation.DS;
import org.springframework.beans.factory.annotation.Autowired;
import whu.edu.cn.dao.NameDao;
import whu.edu.cn.entity.Extent;
import whu.edu.cn.entity.MeasurementName;
import whu.edu.cn.entity.Product;
import whu.edu.cn.mapper.ProductMapper;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.sql.Timestamp;
import java.util.List;

@Service
@DS("postgres")
public class ProductService {
    @Resource
    ProductMapper productMapper;

    public List<Product> getallproducts() {
        List<Product> products = productMapper.getAllProducts();
        products.forEach(product -> product.setMeasurements(productMapper.getMeasurements(Integer.parseInt(product.getProductKey()))));
        return products;
    }

    public List<MeasurementName> getMeasurementsByName(String name){
        List<MeasurementName> measurements = productMapper.getMeasurementNamesByName(name);
        return measurements;
    }

    public Extent getExtentById(Integer id){
        return productMapper.getExtentByID(id);
    }

    public List<Product> getproductsbyname(String ProductName){
        List<Product> products=productMapper.getEOProductsByName(ProductName);
        products.forEach(product -> product.setMeasurements(productMapper.getMeasurements(Integer.parseInt(product.getProductKey()))));
        return products;
    }

    public List<NameDao> getallproductnames(){
        return productMapper.getAllProductNames();
    }

    public List<Product> getproducts(){return productMapper.selectProduct();}

    public List<Product> getproductsbyparams(String ProductName, Timestamp StartTime, Timestamp EndTime,String WKT) {
        List<Product> products = productMapper.getProductsByParams(ProductName,StartTime,EndTime,WKT);
        products.forEach(product -> product.setMeasurements(productMapper.getMeasurements(Integer.parseInt(product.getProductKey()))));
        return  products;
    }

}
