package whu.edu.cn.controller;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.web.bind.annotation.*;
import whu.edu.cn.dao.NameDao;
import whu.edu.cn.dao.ProductDao;
import whu.edu.cn.dao.Result;
import whu.edu.cn.dao.VectorDao;
import whu.edu.cn.entity.Extent;
import whu.edu.cn.entity.MeasurementName;
import whu.edu.cn.entity.Product;
import whu.edu.cn.mapper.ProductMapper;
import whu.edu.cn.service.ProductService;
import whu.edu.cn.service.VectorInfoService;
import whu.edu.cn.util.GeoUtil;
import whu.edu.cn.util.RsqGsUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;
import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;

@CrossOrigin(origins = "*",maxAge = 3600)
@RestController
@Api(tags = "产品查询")
@RequestMapping("/Geocube")
public class GeocubeQuery {
    @Autowired
    ProductService productService;
    @Resource
    ProductMapper productMapper;
    @Autowired
    RsqGsUtil rsqGsUtil;
    @Autowired
    GeoUtil geoUtil;
    @Autowired
    VectorInfoService vectorInfoService;

    @GetMapping("/GetCapabilities")
    @ApiOperation(value = "获取GeoCube所有产品信息",notes = "查询所有产品")
    public Result<?> GetAllProduct(){
        List<Product> products = productService.getallproducts();
        List<ProductDao> productDaos = geoUtil.TransformProducts(products);
        return Result.ok(productDaos);
    }

    @GetMapping("/GetVectorCapabilities")
    @ApiOperation(value = "获取GeoCube所有产品信息",notes = "查询所有产品")
    public Result<?> GetVectorProducts(){
        List<VectorDao> vectorDaos= productMapper.getVectorProducts();
        return Result.ok(vectorDaos);
    }

    @GetMapping("/GetExtent")
    @ApiOperation(value = "根据Key获取Extent",notes = "Extent测试")
    public Extent GetExtentByID(@RequestParam("id") Integer id){
        return productService.getExtentById(id);
    }

    @GetMapping("/GetProductNames")
    @ApiOperation(value = "获取所有产品的Name",notes = "查询所有产品名")
    public Result<?> GetALLProductNames(){
        List<NameDao> nameDaos = productService.getallproductnames();
        return Result.ok(nameDaos);
    }

    @GetMapping("/GetProductsByName")
    @ApiOperation(value = "根据Name获取不同时间空间的产品",notes = "通过名称，类型查询产品")
    public Result<?> GetAllProductsByName(@RequestParam("name") String name,@RequestParam("type") String type){
        if(type.equals("EO")){
            List<Product> products = productService.getproductsbyname(name);
            List<ProductDao> productDaos = geoUtil.TransformProducts(products);
            return Result.ok(productDaos);
        }
        else if(type.equals("Vector")){
            List<VectorDao> vectorDaos=productMapper.getVectorProductsByName(name);
            return Result.ok(vectorDaos);
//            return  GetProductsByParamsAndMeasurement(name,"","","Vector",-180.0,-90.0,180.0,90.0,null);
        } else{
            return Result.error("ProductType Error!");
        }
    }

    @GetMapping("/GetMeasurementsByName")
    @ApiOperation(value = "波段查询",notes = "根据产品名得到波段")
    public Result<?> GetMeasurementsByName(@RequestParam("name") String name){
        List<MeasurementName> measurements = productService.getMeasurementsByName(name);
        return Result.ok(measurements);
    }

    @GetMapping("/GetProducts")
    @ApiOperation(value = "MP sql 测试",notes = "Mybatis测试")
    public List<Product> GetAllProducts(){
        return productService.getproducts();
    }

    @GetMapping("/GetProductsByParams")
    @ApiOperation(value = "时空查询",notes = "测试")
    public Result<?> GetProductsByParams(@RequestParam(value = "productName",required = false) String productName,
                                             @RequestParam(value = "startTime",required = false) String StartTime,
                                             @RequestParam(value = "endTime",required = false) String EndTime,
                                             @RequestParam(value = "measurementName",required = false) String MeasurementName,
                                             @RequestParam("minx")double minx,
                                             @RequestParam("miny")double miny,
                                             @RequestParam("maxx")double maxx,
                                             @RequestParam("maxy")double maxy){
        String WKT_rec="";
        if(minx==-180.0&&miny==-90.0&&maxx==180.0&&maxy==90.0){
            System.out.println("未设置空间范围");
        } else {
            WKT_rec=rsqGsUtil.DoubleToWKT(minx, miny, maxx, maxy);
        }
        System.out.println(WKT_rec);
        System.out.println(productName);
        System.out.println(StartTime);
        System.out.println(EndTime);
        Timestamp  startTime;
        if(StartTime==null||StartTime.equals("")){
            startTime = Timestamp.valueOf("2000-01-01 00:00:00");
        } else{
            startTime = Timestamp.valueOf(StartTime);
        }
        Timestamp endTime;
        if(EndTime==null||EndTime.equals("")){
            endTime = new Timestamp(System.currentTimeMillis());
        } else{
            endTime = Timestamp.valueOf(EndTime);
        }
        System.out.println(startTime);
        System.out.println(endTime);
        System.out.println("measurement is：" + MeasurementName);
        List<Product> products = productService.getproductsbyparams(productName,startTime,endTime,WKT_rec);
        List<ProductDao> productDaos = geoUtil.TransformProducts(products);
        List<ProductDao> collect1 = productDaos.stream().filter(productDao -> productDao.getMeasurementName().equals("Red")).collect(Collectors.toList());
        System.out.println(collect1);
        if(MeasurementName==null||MeasurementName.equals("")){
            return Result.ok(productDaos);
        } else{
            List<ProductDao> collect = productDaos.stream().filter(productDao -> productDao.getMeasurementName().equals(MeasurementName)).collect(Collectors.toList());
            return Result.ok(collect);
        }

    }

    @GetMapping("/GetProductsByParamsAndType")
    @ApiOperation(value = "添加数据类型查询",notes = "测试")
    public Result<?> GetProductsByParamsAndMeasurement(@RequestParam(value = "productName",required = false) String productName,
                                                    @RequestParam(value = "startTime",required = true) String StartTime,
                                                    @RequestParam(value = "endTime",required = true) String EndTime,
                                                    @RequestParam(value = "productType",required = true) String ProductType,
                                                    @RequestParam(value = "minx",required = false,defaultValue = "-180.0")double minx,
                                                    @RequestParam(value = "miny",required = false,defaultValue = "-90.0")double miny,
                                                    @RequestParam(value = "maxx",required = false,defaultValue = "180.0")double maxx,
                                                    @RequestParam(value = "maxy",required = false,defaultValue = "90.0")double maxy,
                                                    @RequestParam(value="measurement",required = false) String measurement){
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
        System.out.println(productName);


        if(ProductType.equals("EO")){
            return  GetProductsByParams(productName,StartTime,EndTime,measurement,minx,miny,maxx,maxy);
        }
        else if(ProductType.equals("Vector")){
            List<JsonNode> jsonNodes = vectorInfoService.queryVectorJsons(productName,minx,miny,maxx,maxy,StartTime,EndTime);
            return Result.ok(jsonNodes);
        } else{
            return Result.error("ProductType Error!");
        }
    }





}
