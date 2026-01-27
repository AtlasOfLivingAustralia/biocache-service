package au.org.ala.biocache.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springdoc.core.SwaggerUiConfigProperties;
import org.springdoc.core.SwaggerUiOAuthProperties;
import org.springdoc.webmvc.ui.SwaggerIndexPageTransformer;
import org.springdoc.webmvc.ui.SwaggerIndexTransformer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.*;
import org.springframework.core.io.Resource;
import org.springframework.web.servlet.resource.ResourceTransformerChain;
import org.springframework.web.servlet.resource.TransformedResource;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

@SpringBootApplication
@ImportResource("classpath:spring.xml")
@ComponentScan( { "au.org.ala.biocache.*" , "au.org.ala.ws.config" })
public class Application extends SpringBootServletInitializer {

    @Value("${server.servlet.context-path:}")
    private String contextPath;

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

    @Bean
    public SwaggerIndexTransformer customIndexTransformer(SwaggerUiConfigProperties swaggerUiConfig, SwaggerUiOAuthProperties swaggerUiOAuthProperties, ObjectMapper objectMapper) {

        return new SwaggerIndexPageTransformer(swaggerUiConfig, swaggerUiOAuthProperties, objectMapper) {

            @Override
            public Resource transform(HttpServletRequest request, Resource resource, ResourceTransformerChain transformerChain) throws IOException {
                Resource transformedResource = super.transform(request, resource, transformerChain);
                String html = new String(transformedResource.getInputStream().readAllBytes());
                String updatedHtml = overwriteSwaggerDefaultUrl(html);
                return new TransformedResource(transformedResource, updatedHtml.getBytes());
            }

            @Override
            protected String overwriteSwaggerDefaultUrl(String html) {
                // replaces petstore url with our api-docs url
                return html.replace("url: \"https://petstore.swagger.io/v2/swagger.json\"",
                        "url: \"" + contextPath + "/v3/api-docs\"");
            }
        };
    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
