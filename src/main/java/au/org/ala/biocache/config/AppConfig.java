package au.org.ala.biocache.config;

import au.org.ala.biocache.service.NameMatchSpeciesLookupService;
import au.org.ala.biocache.service.RestartDataService;
import au.org.ala.biocache.service.SpeciesLookupService;
import au.org.ala.dataquality.api.QualityServiceRpcApi;
import au.org.ala.dataquality.client.ApiClient;
import au.org.ala.names.ws.client.ALANameUsageMatchServiceClient;
import au.org.ala.ws.ClientConfiguration;
import org.apache.catalina.Context;
import org.apache.log4j.Logger;
import org.apache.tomcat.util.scan.StandardJarScanner;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.AbstractMessageSource;
import org.springframework.web.servlet.ViewResolver;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.view.InternalResourceViewResolver;
import org.springframework.web.servlet.view.JstlView;
import org.springframework.boot.actuate.trace.http.InMemoryHttpTraceRepository;
import org.springframework.boot.actuate.trace.http.HttpTraceRepository;
import javax.inject.Inject;
import java.io.IOException;
import java.net.URL;

/**
 * This class handles the switching between implementations of interfaces based on
 * external configuration.
 */
@Configuration
@EnableCaching
public class AppConfig implements WebMvcConfigurer {

    private final static Logger logger = Logger.getLogger(AppConfig.class);

    @Inject
    private AbstractMessageSource messageSource; // use for i18n of the headers

    // Configuration for DataQualityApi
    @Value("${dataquality.baseUrl:https://dataquality.ala.org.au/}")
    protected String dataQualityBaseUrl;

    //Set RestartDataService.dir before classes using RestartDataService are instantiated.
    @Value("${restart.data.dir:/tmp}")
    public void setDatabase(String dir) {
        logger.debug("setting RestartDataService.dir: " + dir);
        RestartDataService.dir = dir;
    }

    @Value("${namesearch.url:http://localhost:9179}")
    String nameSearchUrl = "http://localhost:9179";

    @Value("${namesearch.timeout:30}")
    Integer nameSearchTimeout = 30;

    @Value("${namesearch.cache.size:50}")
    Integer nameSearchCacheSize = 50;

    @Bean
    public HttpTraceRepository httpTraceRepository() {
        return new InMemoryHttpTraceRepository();
    }

    public @Bean(name = "nameUsageMatchService")
    ALANameUsageMatchServiceClient nameUsageMatchService() throws IOException {

        ClientConfiguration clientConfiguration =
                ClientConfiguration.builder()
                        .baseUrl(new URL(nameSearchUrl))
                        .timeOut(nameSearchTimeout * 1000) // Geocode service connection time-out
                        .cacheSize(nameSearchCacheSize * 1024 * 1024)
                        .build();

        return new ALANameUsageMatchServiceClient(clientConfiguration);
    }

    //https://stackoverflow.com/questions/43370840/disable-scanmanifest-of-jar-scan-in-tomcat-embed-in-spring-boot
    @Bean
    public TomcatServletWebServerFactory tomcatFactory() {
        return new TomcatServletWebServerFactory() {
            @Override
            protected void postProcessContext(Context context) {
                ((StandardJarScanner) context.getJarScanner()).setScanManifest(false);
            }
        };
    }

    protected SpeciesLookupService getNameMatchSpeciesLookupService() {
        logger.info("Initialising name match species lookup services.");
        NameMatchSpeciesLookupService service = new NameMatchSpeciesLookupService();
        service.setMessageSource(messageSource);
        return service;
    }

    public @Bean(name = "speciesLookupService")
    SpeciesLookupService speciesLookupServiceBean() {
        logger.info("Initialising species lookup services.");
        return getNameMatchSpeciesLookupService();
    }

    @Bean("dataQualityApiClient")
    public ApiClient dataQualityApiClient() {
        ApiClient apiClient = new ApiClient();
        apiClient.getAdapterBuilder().baseUrl(dataQualityBaseUrl);
        return apiClient;
    }

    @Bean
    public QualityServiceRpcApi dataQualityApi(@Qualifier("dataQualityApiClient") ApiClient dataQualityApiClient) {
        return dataQualityApiClient.createService(QualityServiceRpcApi.class);
    }

    @Bean
    public ViewResolver internalResourceViewResolver() {
        InternalResourceViewResolver bean = new InternalResourceViewResolver();
        bean.setViewClass(JstlView.class);
        bean.setPrefix("/WEB-INF/jsp/");
        bean.setSuffix(".jsp");
        return bean;
    }

    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        registry.addRedirectViewController("/", "/swagger-ui.html");
    }
//
//    @Override
//    public void configureDefaultServletHandling(DefaultServletHandlerConfigurer configurer) {
//        configurer.enable();
//    }
}
