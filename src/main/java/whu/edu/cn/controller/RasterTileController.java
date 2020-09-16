package whu.edu.cn.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import whu.edu.cn.service.RasterTileService;

import javax.servlet.http.HttpSession;
import java.text.ParseException;

@CrossOrigin(origins = "*",maxAge = 3600)
@RestController
@RequestMapping("/Geocube")
@Api(tags = "瓦片查询")
public class RasterTileController {
    static final String paramsPath = "/home/geocube/data/conf/requestParams";

    @Autowired
    RasterTileService rasterTileService;

    //前台预先传递部分查询参数, 包括产品名、时间、波段, 后台写入配置文件
    // @RequestMapping(value = "/queryParams")
    @GetMapping(value = "/queryParams")
    @ApiOperation(value = "瓦片请求预参数",notes = "将产品名、时间、波段, 后台写入配置文件")
    public String fixParams(@RequestParam(value = "rasterProductName", required = true) String rasterProductName,
                            @RequestParam(value = "time", required = true) String time,
                            @RequestParam(value = "measurement", required = true) String measurement,
                            HttpSession session) {
        session.setAttribute("rasterProductName",rasterProductName);
        session.setAttribute("time",time);
        session.setAttribute("measurement",measurement);
        System.out.println("queryParams session ID: " + session.getId());
        //rasterTileService.writeQueryParams(rasterProductName, time, measurement, paramsPath);
        return "Get parameters successfully!";
    }

    //根据层、列、行空间信息,结合配置文件中参数, 实时返回瓦片png
    //@RequestMapping(value = "/getRasterTile/{z}/{x}/{y}.png",produces = MediaType.IMAGE_PNG_VALUE)
    //@ResponseBody
    @ApiOperation(value = "请求瓦片",notes = "根据层、列、行空间信息,结合配置文件参数")
    @GetMapping(value = "/getRasterTile/{z}/{x}/{y}.png",produces = MediaType.IMAGE_PNG_VALUE)
    public byte[] getRasterTile(@PathVariable("z") int level,
                                @PathVariable("x") int column,
                                @PathVariable("y") int row,
                                HttpSession session) throws ParseException {
        System.out.println("level = " + level + ",column = " + column + ",row = " + row);
        System.out.println("rasterProductName = " + session.getAttribute("rasterProductName") +
                ",time = " + session.getAttribute("time") +
                ",measurement = " + session.getAttribute("measurement"));
        System.out.println("getRasterTile session ID: " + session.getId());
        byte[] bytes = rasterTileService.queryPyramidTile(level, column, row,
                session.getAttribute("rasterProductName").toString(),
                session.getAttribute("time").toString(),
                session.getAttribute("measurement").toString());
        return bytes;
    }
    @ApiOperation(value = "测试瓦片",notes = "测试瓦片查询效率")
    @GetMapping(value = "/RasterTilesTest")
    public String getRasterTile(@RequestParam(value = "productName",required = true,defaultValue = "LC08_L1TP_ARD_EO") String productName,
                                @RequestParam(value = "startTime",required = true) String StartTime,
                                @RequestParam(value = "endTime",required = true) String EndTime,
                                @RequestParam(value = "level",required = true) String Level,
                                @RequestParam(value = "minx",required = false,defaultValue = "116.01494046724021")double minx,
                                @RequestParam(value = "miny",required = false,defaultValue = "33.073457222586285")double miny,
                                @RequestParam(value = "maxx",required = false,defaultValue = "117.9181165740333")double maxx,
                                @RequestParam(value = "maxy",required = false,defaultValue = "32.9597805438586")double maxy,
                                @RequestParam(value="measurement",required = false) String[] measurements) throws ParseException {

        return rasterTileService.queryTilesTest(minx,miny,maxx,maxy,productName,StartTime,EndTime,Level,measurements);
    }

    /*Test
    @RequestMapping(value = "/getTile",produces = MediaType.IMAGE_PNG_VALUE)
    @ResponseBody
    public byte[] getTile() throws IOException {
        String path = "F:\\Scala\\IDAE_Env\\out\\artifacts\\TileQuery_Env_jar\\DcTiles\\1516559446000_56_299.png";
        byte[] bytes = new byte[1024*1024];
        FileInputStream fis = new FileInputStream(new File(path));
        fis.read(bytes);
        return bytes;
    }*/

}


