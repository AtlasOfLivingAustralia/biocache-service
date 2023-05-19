package au.org.ala.biocache.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.PropertySource;

@SpringBootApplication
@ImportResource("classpath:spring.xml")
@ComponentScan( { "au.org.ala.biocache.*" , "au.org.ala.ws.config" })
@PropertySource(value="file:///data/biocache/config/biocache-config.properties", ignoreResourceNotFound=true)
public class Application extends SpringBootServletInitializer {

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        return application.sources(Application.class);
    }

    @Bean
    public OpenAPI customOpenAPI(
            @Value("${application-title}") String appTitle,
            @Value("${application-description}") String appDescription,
            @Value("${application-version}") String appVersion,
            @Value("${application-terms-url}") String appTermsUrl
            ) {
        return new OpenAPI()
                .info(new Info()
                                .title(appTitle)
                                .version(appVersion)
                                .description(appDescription)
                                .termsOfService(appTermsUrl));
    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}