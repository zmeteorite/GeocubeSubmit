package whu.edu.cn.service;

import geotrellis.raster.Tile;
import org.springframework.stereotype.Service;
import scala.Tuple2;
import whu.edu.cn.query.service.QueryRasterTiles;
import whu.edu.cn.query.entity.Extent;
import whu.edu.cn.query.entity.QueryParams;
import whu.edu.cn.query.entity.SpaceTimeBandKey;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

//import whu.edu.cn.query.entity.QueryParams;
//import whu.edu.cn.query.service.QueryTiles;

@Service
public class RasterTileService {
    public byte[] queryPyramidTile(int level,
                            int column,
                            int row,
                            String rasterProductName,
                            String time,
                            String measurement) throws ParseException{
        String gridCode = Extent.xy2GridCode(column, row, level);

        QueryParams queryParams = new QueryParams();
        queryParams.setGridCodes(new String[]{gridCode});
        queryParams.setLevel(String.valueOf(level));
        queryParams.setRasterProductName(rasterProductName);
        queryParams.setMeasurements(new String[]{measurement});

        SimpleDateFormat sj = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date d = sj.parse(time);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(d);
        calendar.add(Calendar.SECOND, 1);
        String nextInstant = sj.format(calendar.getTime());
        calendar.add(Calendar.SECOND, -2);
        String previousInstant = sj.format(calendar.getTime());
        queryParams.setTime(previousInstant, nextInstant);

        /*queryParams.setRasterProductName("LC08_L1TP_ARD");
        queryParams.setTime("2013-04-14 02:32:35.004", "2013-04-14 02:32:37.004"); //2013-04-14 02:32:36.004
        queryParams.setMeasurements(new String[]{"Green"});*/

        /*Tuple2<SpaceTimeBandKey, Tile> tile = QueryRasterTiles.getTiles(queryParams)._1[0];
        return whu.edu.cn.query.util.TileSerializer.tile2PngBytes(tile._2);*/

        return QueryRasterTiles.getPyramidTile(queryParams);  //直接返回png bytes
    }

    /*public byte[] queryTile(int level,
                            int column,
                            int row,
                            String rasterProductName,
                            String time,
                            String measurement) throws ParseException{
        String gridCode = whu.edu.cn.query.entity.Extent.xy2GridCode(column, row);

        whu.edu.cn.query.entity.QueryParams queryParams = new whu.edu.cn.query.entity.QueryParams();
        queryParams.setGridCodes(new String[]{gridCode});
        queryParams.setLevel(String.valueOf(level));
        queryParams.setRasterProductName(rasterProductName);
        queryParams.setMeasurements(new String[]{measurement});

        SimpleDateFormat sj = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date d = sj.parse(time);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(d);
        calendar.add(Calendar.SECOND, 1);
        String nextInstant = sj.format(calendar.getTime());
        calendar.add(Calendar.SECOND, -2);
        String previousInstant = sj.format(calendar.getTime());
        queryParams.setTime(previousInstant, nextInstant);

        *//*queryParams.setRasterProductName("LC08_L1TP_ARD");
        queryParams.setTime("2013-04-14 02:32:35.004", "2013-04-14 02:32:37.004"); //2013-04-14 02:32:36.004
        queryParams.setMeasurements(new String[]{"Green"});*//*

        Tuple2<SpaceTimeKey, Tuple2<String,Tile>> tile = whu.edu.cn.query.service.QueryTiles.getTiles(queryParams)._1[0];
        return whu.edu.cn.query.util.TileSerializer.tile2PngBytes(tile._2._2);
    }*/

    /*public byte[] queryTile(int level, int column, int row, String paramsPath) throws IOException, ParseException{
        String gridCode = whu.edu.cn.query.entity.Extent.xy2GridCode(column, row);

        QueryParams queryParams = new QueryParams();
        queryParams.setGridCodes(new String[]{gridCode});
        queryParams.setLevel(String.valueOf(level));

        readQueryParams(queryParams, paramsPath);
        *//*queryParams.setRasterProductName("LC08_L1TP_ARD");
        queryParams.setTime("2013-04-14 02:32:35.004", "2013-04-14 02:32:37.004"); //2013-04-14 02:32:36.004
        queryParams.setMeasurements(new String[]{"Green"});*//*

        Tuple2<SpaceTimeKey, Tuple2<String,Tile>> tile = QueryTiles.getTiles(queryParams)._1[0];
        return whu.edu.cn.query.util.TileSerializer.tile2PngBytes(tile._2._2);
    }

    public void readQueryParams(QueryParams queryParams, String paramsPath) throws IOException, ParseException {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(paramsPath))));
        String[] params = br.readLine().split(",");
        queryParams.setRasterProductName(params[0]);
        queryParams.setMeasurements(new String[]{params[2]});

        String time = params[1];
        SimpleDateFormat sj = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date d = sj.parse(time);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(d);
        calendar.add(Calendar.SECOND, 1);
        String nextInstant = sj.format(calendar.getTime());
        //System.out.println("nextInstant: " + nextInstant);
        calendar.add(Calendar.SECOND, -2);
        String previousInstant = sj.format(calendar.getTime());
        //System.out.println("previousInstant: " + previousInstant);

        queryParams.setTime(previousInstant, nextInstant);
    }

    public void writeQueryParams(String rasterProductName,
                                 String time,
                                 String measurement,
                                 String paramsPath) throws IOException{
        File file = new File(paramsPath);
        FileWriter fileWriter = new FileWriter(file, false);
        fileWriter.write(rasterProductName + "," + time + "," + measurement);
        fileWriter.flush();
        fileWriter.close();
    }*/

    /*public static void main(String[] args) throws ParseException {
        String time = "2013-04-14 02:32:36.004";
        SimpleDateFormat sj = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date d = sj.parse(time);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(d);
        calendar.add(Calendar.SECOND, 1);
        String nextInstant = sj.format(calendar.getTime());
        System.out.println("nextInstant: " + nextInstant);
        calendar.add(calendar.SECOND, -2);
        String previousInstant = sj.format(calendar.getTime());
        System.out.println("previousInstant: " + previousInstant);
    }*/

}
