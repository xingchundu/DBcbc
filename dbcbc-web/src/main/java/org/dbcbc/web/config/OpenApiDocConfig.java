/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbcbc.web.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiDocConfig {

    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("DBCDC API")
                        .version("2.0.0")
                        .description("DBCDC 数据同步平台接口文档"));
    }
}
