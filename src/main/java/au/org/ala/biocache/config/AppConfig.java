package au.org.ala.biocache.config;

import au.org.ala.biocache.service.FacetService;
import au.org.ala.biocache.service.SpeciesLookupIndexService;
import au.org.ala.biocache.service.SpeciesLookupRestService;
import au.org.ala.biocache.service.SpeciesLookupService;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.AbstractMessageSource;
import org.springframework.web.client.RestOperations;

import javax.inject.Inject;

/**
 * This class handles the switching between implementations of interfaces based on
 * external configuration.
 */
@Configuration
public class AppConfig {

    private final static Logger logger = Logger.getLogger(AppConfig.class);

    @Inject
    private AbstractMessageSource messageSource; // use for i18n of the headers

    @Value("${name.index.dir:/data/lucene/namematching}")
    protected String nameIndexLocation;

    @Inject
    @Qualifier("restTemplate")
    private RestOperations restTemplate; // NB MappingJacksonHttpMessageConverter() injected by Spring

    /** URI prefix for bie-service - may be overridden in properties file */
    @Value("${service.bie.ws.url:http://bie.ala.org.au/ws}")
    protected String bieUriPrefix;

    //NC 20131018: Allow service to be disabled via config (enabled by default)
    @Value("${service.bie.enabled:false}")
    protected Boolean enabled;

    // Configuration for facets
    @Value("${facet.config:/data/biocache/config/facets.json}")
    protected String facetConfig;
    @Value("${facets.max:4}")
    protected Integer facetsMax;
    @Value("${facet.default:true}")
    protected Boolean facetDefault;


    public @Bean(name = "speciesLookupService")
    SpeciesLookupService speciesLookupServiceBean() {
        logger.info("Initialising species lookup services.");
        if(enabled){
            logger.info("Initialising rest-based species lookup services.");
            SpeciesLookupRestService service = new SpeciesLookupRestService();
            service.setBieUriPrefix(bieUriPrefix);
            service.setEnabled(enabled);
            service.setRestTemplate(restTemplate);
            service.setMessageSource(messageSource);
            return service;
        } else {
            logger.info("Initialising local index-based species lookup services.");
            SpeciesLookupIndexService service = new SpeciesLookupIndexService();
            service.setNameIndexLocation(nameIndexLocation);
            service.setMessageSource(messageSource);
            return service;
        }
    }

    public @Bean(name = "facetService")
    FacetService facetServiceBean() {
        logger.info("Setting up configured facet service");
        FacetService.createSingleton(this.facetConfig, this.facetsMax, this.facetDefault);
        return FacetService.singleton();
    }
}
