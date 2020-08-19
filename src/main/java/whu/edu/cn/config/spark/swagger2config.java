package whu.edu.cn.config.spark;

import io.swagger.annotations.ApiOperation;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@Configuration
@EnableSwagger2
public class swagger2config {
    /**
     *用于配置swagger2，包含文档基本信息
     * 指定swagger2的作用于（这里指定包路径下的所有API）
     * @return Docket
     */
    @Bean
    public Docket createRestApi(){
        return new Docket(DocumentationType.SWAGGER_2)
                .apiInfo(apiInfo())
                .select()
                .apis(RequestHandlerSelectors.basePackage("whu.edu.cn.controller"))
                //加了ApiOperation注解的类，才生成接口文档
                .apis(RequestHandlerSelectors.withMethodAnnotation(ApiOperation.class))
                .paths(PathSelectors.any())
                .build();
    }
    /**
     * 构建文档基本信息，用于页面显示，可以包含版本、
     * 联系人信息，服务地址，文档描述信息等
     * @return ApiInfo
     */

    private ApiInfo apiInfo(){
        return new ApiInfoBuilder()
                .title("Geocube-Boot 后台服务API接口文档")
                .description("后台API接口")
                .version("1.0")
                .contact("武汉大学")
                .license("The Apache License, Version 2.0")
                .licenseUrl("http://www.apache.org/licenses/LICENSE-2.0.html")
                .build();
    }
}

