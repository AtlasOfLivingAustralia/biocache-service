package au.org.ala.biocache.config;

import au.org.ala.biocache.index.IndexDAO;
import au.org.ala.biocache.index.SolrIndexDAO;
import au.org.ala.biocache.service.RestartDataService;
import au.org.ala.biocache.service.SpeciesLookupIndexService;
import au.org.ala.biocache.service.SpeciesLookupRestService;
import au.org.ala.biocache.service.SpeciesLookupService;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.AbstractMessageSource;
import org.springframework.web.client.RestOperations;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
    @Value("${service.bie.ws.url:https://bie-ws.ala.org.au/ws}")
    protected String bieUriPrefix;

    //NC 20131018: Allow service to be disabled via config (enabled by default)
    @Value("${service.bie.enabled:false}")
    protected Boolean enabled;

    //Disable the default that autocomplete uses the local names index. For use when there are no local names index files.
    @Value("${service.autocomplete.local.enabled:true}")
    protected Boolean autocompleteLocalEnabled;

    // Configuration for facets
    @Value("${facet.config:/data/biocache/config/facets.json}")
    protected String facetConfig;
    @Value("${facets.max:4}")
    protected Integer facetsMax;
    @Value("${facet.default:true}")
    protected Boolean facetDefault;


    //Set RestartDataService.dir before classes using RestartDataService are instantiated.
    @Value("${restart.data.dir:/tmp}")
    public void setDatabase(String dir) {
        logger.debug("setting RestartDataService.dir: " + dir);
        RestartDataService.dir = dir;
    }


    protected SpeciesLookupService getSpeciesLookupRestService() {
        logger.info("Initialising rest-based species lookup services.");
        SpeciesLookupRestService service = new SpeciesLookupRestService();
        service.setBieUriPrefix(bieUriPrefix);
        service.setEnabled(enabled);
        service.setRestTemplate(restTemplate);
        service.setMessageSource(messageSource);
        return service;
    }

    protected SpeciesLookupService getSpeciesLookupIndexService() {
        logger.info("Initialising local index-based species lookup services.");
        SpeciesLookupIndexService service = new SpeciesLookupIndexService();
        service.setNameIndexLocation(nameIndexLocation);
        service.setMessageSource(messageSource);
        return service;
    }

    public @Bean(name = "speciesLookupService")
    SpeciesLookupService speciesLookupServiceBean() {
        logger.info("Initialising species lookup services.");
        if(enabled){
            return getSpeciesLookupRestService();
        } else {
            return getSpeciesLookupIndexService();
        }
    }

    public @Bean(name = "speciesLookupIndexService")
    SpeciesLookupService speciesLookupIndexService() {
        logger.info("Initialising species lookup services.");
        try {
            if (autocompleteLocalEnabled) {
                return getSpeciesLookupIndexService();
            }
        } catch (Exception e) {
            logger.error("Failed to initialise local species lookup service for use with the species autocomplete ws. Attempting to use BIE instead.");
        }
        if (enabled) {
            return getSpeciesLookupRestService();
        } else {
            return null;
        }
    }

    public @Bean(name = "solrClient")
    SolrClient solrClientBean() {

        //TODO - this is experimental. Requires more configuration params for tuning timeouts etc
        String solrHome = au.org.ala.biocache.Config.solrHome();
        if (!solrHome.startsWith("http")) {
            String[] zkHosts = solrHome.split(",");
            List<String> hosts = new ArrayList<String>();
            for (String zkHost: zkHosts){
                hosts.add(zkHost.trim());
            }
            //HTTP2
            CloudHttp2SolrClient.Builder builder = new CloudHttp2SolrClient.Builder(hosts, Optional.empty());
            CloudHttp2SolrClient client = builder.build();
            client.setDefaultCollection(au.org.ala.biocache.Config.solrCollection());
            return client;
        } else {
            Http2SolrClient.Builder builder = new Http2SolrClient.Builder(au.org.ala.biocache.Config.solrHome());
            builder.connectionTimeout(au.org.ala.biocache.Config.solrConnectionConnectTimeout());
            builder.maxConnectionsPerHost(au.org.ala.biocache.Config.solrConnectionMaxPerRoute());
            return builder.build();
        }


//        SolrClient result;
//
//        SolrIndexDAO dao = (SolrIndexDAO) au.org.ala.biocache.Config
//                .getInstance(IndexDAO.class);
//        dao.init();
//        result = dao.solrServer();
//
//        if (logger.isDebugEnabled()) {
//            logger.debug("solrClient initialised, Type: " + result.getClass());
//        }
//
//        return result;
    }
}
