package whu.edu.cn.controller;

import whu.edu.cn.dao.ProductDao;
import whu.edu.cn.entity.Extent;
import whu.edu.cn.entity.MeasurementName;
import whu.edu.cn.entity.Product;
import whu.edu.cn.service.ProductService;
import whu.edu.cn.util.GeoUtil;
import whu.edu.cn.util.RsqGsUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@Api(tags = "产品查询")
@RequestMapping("/Geocube")
public class GeocubeQuery {
    @Autowired
    ProductService productService;
    @Autowired
    RsqGsUtil rsqGsUtil;
    @Autowired
    GeoUtil geoUtil;

    @GetMapping("/GetCapabilities")
    @ApiOperation(value = "获取GeoCube所有产品信息",notes = "查询所有产品")
    public List<ProductDao> GetAllProduct(){
        List<Product> products = productService.getallproducts();
        List<ProductDao> productDaos = geoUtil.TransformProducts(products);
        return productDaos;
    }

    @GetMapping("/GetExtent")
    @ApiOperation(value = "根据Key获取Extent",notes = "Extent测试")
    public Extent GetExtentByID(@RequestParam("id") Integer id){
        return productService.getExtentById(id);
    }

    @GetMapping("/GetProductNames")
    @ApiOperation(value = "获取所有产品的Name",notes = "查询所有产品名")
    public List<String> GetALlProductNames(){
        return productService.getallproductnames();
    }

    @GetMapping("/GetProductsByName")
    @ApiOperation(value = "根据Name获取不同时间空间的产品",notes = "通过名称查询产品")
    public List<ProductDao> GetAllProductsByName(@RequestParam("name") String name){
        List<Product> products = productService.getproductsbyname(name);
        List<ProductDao> productDaos = geoUtil.TransformProducts(products);
        return productDaos;
    }

    @GetMapping("/GetMeasurementsByName")
    @ApiOperation(value = "波段查询",notes = "根据产品名得到波段")
    public List<MeasurementName> GetMeasurementsByName(@RequestParam("name") String name){
        List<MeasurementName> measurements = productService.getMeasurementsByName(name);
        return measurements;
    }

    @GetMapping("/GetProducts")
    @ApiOperation(value = "MP sql 测试",notes = "Mybatis测试")
    public List<Product> GetAllProducts(){
        return productService.getproducts();
    }

    @GetMapping("/GetProductsByParams")
    @ApiOperation(value = "时空查询",notes = "测试")
    public List<ProductDao> GetProductsByParams(@RequestParam(value = "productName",required = false) String productName,
                                             @RequestParam(value = "startTime",required = false) String StartTime,
                                             @RequestParam(value = "endTime",required = false) String EndTime,
                                             @RequestParam(value = "measurementName",required = false) String MeasurementName,
                                             @RequestParam("minx")double minx,
                                             @RequestParam("miny")double miny,
                                             @RequestParam("maxx")double maxx,
                                             @RequestParam("maxy")double maxy){
        //先判断参数是否正确
        if (minx>180||minx<-180){
            System.out.println(minx+"超出经度范围！");
            return null;
        }
        if (miny>90||miny<-90){
            System.out.println(miny+"超出经度范围！");
            return null;
        }
        if (maxx>180||maxx<-180){
            System.out.println(maxx+"超出经度范围！");
            return null;
        }
        if (maxy>180||maxy<-180){
            System.out.println(maxy+"超出经度范围！");
            return null;
        }
        String WKT_rec=rsqGsUtil.DoubleToWKT(minx, miny, maxx, maxy);
        System.out.println(WKT_rec);
        System.out.println(StartTime);
        System.out.println(EndTime);
        Timestamp  startTime;
        if(StartTime.equals("")){
            startTime = Timestamp.valueOf("2000-01-01 00:00:00");
        } else{
            startTime = Timestamp.valueOf(StartTime);
        }
        Timestamp endTime;
        if(EndTime.equals("")){
            endTime = new Timestamp(System.currentTimeMillis());
        } else{
            endTime = Timestamp.valueOf(EndTime);
        }
        System.out.println(startTime);
        System.out.println(productName);
        System.out.println("measurement is：" +MeasurementName);
        List<Product> products = productService.getproductsbyparams(productName,startTime,endTime,WKT_rec);
//        List<Product> products = productService.getproductsbyparams2(productName,startTime,endTime,WKT_rec,measurementName);
        List<ProductDao> productDaos = geoUtil.TransformProducts(products);
        List<ProductDao> collect1 = productDaos.stream().filter(productDao -> productDao.getMeasurementName().equals("Red")).collect(Collectors.toList());
        System.out.println(collect1);
        if(MeasurementName.equals("")){
            System.out.println("measurement is：" +MeasurementName);
            return productDaos;
        } else{
            System.out.println("measurement is：" +MeasurementName);
            List<ProductDao> collect = productDaos.stream().filter(productDao -> productDao.getMeasurementName().equals(MeasurementName)).collect(Collectors.toList());
            return collect;
        }

    }

    @GetMapping("/GetProductsByParamsAndType")
    @ApiOperation(value = "添加数据类型查询",notes = "测试")
    public void GetProductsByParamsAndMeasurement(@RequestParam(value = "productName",required = false) String productName,
                                                @RequestParam(value = "startTime",required = false) String StartTime,
                                                @RequestParam(value = "endTime",required = false) String EndTime,
                                                  @RequestParam(value = "productType",required = true) String ProductType,
                                                @RequestParam("minx")double minx,
                                                @RequestParam("miny")double miny,
                                                @RequestParam("maxx")double maxx,
                                                @RequestParam("maxy")double maxy,
                                                @RequestParam("measurement") String measurement){
        if(ProductType.equals("EO")){
            GetProductsByParams(productName,StartTime,EndTime,measurement,minx,miny,maxx,maxy);
        }
        System.out.println(productName+StartTime+measurement);
    }





}
