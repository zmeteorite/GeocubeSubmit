package whu.edu.cn.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import java.util.Date;

/**
 * @Description: 高分产品归档信息表
 * @Author: jeecg-boot
 * @Date:   2019-12-13
 * @Version: V1.0
 */
@Data
@TableName("rsq_gf_product_inf")
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
@ApiModel(value="rsq_gf_product_inf对象", description="高分产品归档信息表")
public class RsqGFProductInf {
    
	/**id*/
	@TableId(type = IdType.UUID)
    @ApiModelProperty(value = "id")
	private Integer id;
	/**productId*/

    @ApiModelProperty(value = "productId")
	private String productId;
	/**satelliteName*/

    @ApiModelProperty(value = "satelliteName")
	private String satelliteName;
	/**stationName*/

    @ApiModelProperty(value = "stationName")
	private String stationName;
	/**sensorName*/

    @ApiModelProperty(value = "sensorName")
	private String sensorName;
	/**receiveTime*/
    @ApiModelProperty(value = "receiveTime")
	private Date receiveTime;
	/**orbitId*/

    @ApiModelProperty(value = "orbitId")
	private String orbitId;
	/**sceneId*/

    @ApiModelProperty(value = "sceneId")
	private String sceneId;
	/**stripId*/

    @ApiModelProperty(value = "stripId")
	private String stripId;
	/**widthInPixels*/

    @ApiModelProperty(value = "widthInPixels")
	private Integer widthInPixels;
	/**heightInPixels*/

    @ApiModelProperty(value = "heightInPixels")
	private Integer heightInPixels;
	/**imageGsd*/

    @ApiModelProperty(value = "imageGsd")
	private Integer imageGsd;
	/**bands*/

    @ApiModelProperty(value = "bands")
	private Integer bands;
	/**produceTime*/
    @ApiModelProperty(value = "produceTime")
	private Date produceTime;
	/**cloudPercent*/

	/**geom*/
    @ApiModelProperty(value = "geom")
	private Object geom;
	/**productLevel*/
    @ApiModelProperty(value = "productLevel")
	private String productLevel;
	/**startTime*/
    @ApiModelProperty(value = "startTime")
	private Date startTime;
	/**endTime*/
    @ApiModelProperty(value = "endTime")
	private Date endTime;
	/**centerTime*/
    @ApiModelProperty(value = "centerTime")
	private Date centerTime;
	/**centerLat*/
    @ApiModelProperty(value = "centerLat")
	private java.math.BigDecimal centerLat;

	/**centerLong*/
    @ApiModelProperty(value = "centerLong")
	private java.math.BigDecimal centerLong;

	/**upperLeftLat*/
    @ApiModelProperty(value = "upperLeftLat")
	private java.math.BigDecimal upperLeftLat;

	/**upperLeftLong*/
    @ApiModelProperty(value = "upperLeftLong")
	private java.math.BigDecimal upperLeftLong;

	/**upperRightLat*/
    @ApiModelProperty(value = "upperRightLat")
	private java.math.BigDecimal upperRightLat;

	/**upperRightLong*/
    @ApiModelProperty(value = "upperRightLong")
	private java.math.BigDecimal upperRightLong;

	/**lowerLeftLat*/
    @ApiModelProperty(value = "lowerLeftLat")
	private java.math.BigDecimal lowerLeftLat;

	/**lowerLeftLong*/
    @ApiModelProperty(value = "lowerLeftLong")
	private java.math.BigDecimal lowerLeftLong;
	/**lowerRightLat*/

    @ApiModelProperty(value = "lowerRightLat")
	private java.math.BigDecimal lowerRightLat;

	/**lowerRightLong*/
    @ApiModelProperty(value = "lowerRightLong")
	private java.math.BigDecimal lowerRightLong;

}
