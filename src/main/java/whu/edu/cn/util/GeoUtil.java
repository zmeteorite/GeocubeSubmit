package whu.edu.cn.util;

import whu.edu.cn.dao.ProductDao;
import whu.edu.cn.entity.Measurement;
import whu.edu.cn.entity.Product;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class GeoUtil {
    public List<ProductDao> TransformProducts(List<Product> productList){
        List<ProductDao> productDaos = new ArrayList<ProductDao>();
        for(Product product:productList){
            List<Measurement> measurements= product.getMeasurements();
            if(measurements.size()>=1){
                for(Measurement measurement:measurements){
                    System.out.println(product.getProductName());
                    System.out.println(measurement.getMeasurementName());
                    ProductDao productDao = new ProductDao();
                    productDao.setDType(measurement.getDType());
                    productDao.setMeasurementName(measurement.getMeasurementName());

                    productDao.setProductKey(product.getProductKey());
                    productDao.setProductName(product.getProductName());
                    productDao.setPlatformName(product.getPlatformName());
                    productDao.setSensorName(product.getSensorName());
                    productDao.setPhenomenonTime(product.getPhenomenonTime());
                    productDao.setResultTime(product.getResultTime());

                    productDao.setCRS(product.getCRS());
//                    productDao.setCellres(product.getCellres());
//                    productDao.setTilesize(product.getTilesize());
//                    productDao.setLevel(product.getLevel());


                    productDao.setImagingLength(product.getImagingLength());
                    productDao.setImagingWidth(product.getImagingWidth());
                    productDao.setLowerLeftLong(product.getLowerLeftLong());
                    productDao.setLowerLeftLat(product.getLowerLeftLat());
                    productDao.setLowerRightLat(product.getLowerRightLat());
                    productDao.setLowerRightLong(product.getLowerRightLong());
                    productDao.setUpperRightLong(product.getUpperRightLong());
                    productDao.setUpperRightLat(product.getUpperRightLat());
                    productDao.setUpperLeftLong(product.getUpperLeftLong());
                    productDao.setUpperLeftLat(product.getUpperLeftLat());
//                    System.out.println(productDao);
                    productDaos.add(productDao);
                }
            }
        }
        System.out.println(productDaos.size());
        Map<String,List<ProductDao>> collect = productDaos.stream().collect(Collectors.groupingBy(e->fetchGroupKey(e)));
//        System.out.println(collect);
//        System.out.println(collect.values());
        List<ProductDao> productDaoReturn = new ArrayList<ProductDao>();
        for(List<ProductDao> productDaos1:collect.values()){
            System.out.println(productDaos1.get(0));
            productDaoReturn.add(productDaos1.get(0));
        }
//        productDaos.stream().collect(Collectors.groupingBy(e->fetchGroupKey(e)));
        return productDaoReturn;
    }

    public static String fetchGroupKey(ProductDao productDao){
        return productDao.getPhenomenonTime()+"#"+productDao.getProductName()+"#"+productDao.getLowerLeftLong()+"#"+
                productDao.getLowerLeftLat()+"#"+productDao.getUpperRightLong()+"#"+productDao.getUpperRightLat()+"$"+productDao.getMeasurementName();
    }
}
