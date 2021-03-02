package au.org.ala.biocache.dao;

import au.org.ala.biocache.dto.*;
import au.org.ala.biocache.service.LayersService;
import au.org.ala.biocache.service.RestartDataService;
import au.org.ala.biocache.util.DwCTerms;
import au.org.ala.biocache.util.DwcTermDetails;
import au.org.ala.biocache.util.QueryFormatUtils;
import au.org.ala.biocache.util.solr.FieldMappingUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.cache.CacheConfig;
import org.apache.http.impl.client.cache.CachingHttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.RangeFacet;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.support.AbstractMessageSource;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;

/**
 * SOLR index.
 */
@Component("indexDao")
public class SolrIndexDAOImpl implements IndexDAO {
  /**
   * log4j logger
   */
  private static final Logger logger = Logger.getLogger(SolrIndexDAOImpl.class);

  @Inject
  protected LayersService layersService;

  /**
   * SOLR index version for client app caching use.
   */
  private volatile long solrIndexVersion = 0;
  /**
   * last time SOLR index version was refreshed
   */
  private volatile long solrIndexVersionTime = 0;
  /**
   * Lock object used to synchronize updates to the solr index version
   */
  private final Object solrIndexVersionLock = new Object();

  @Inject
  private FieldMappingUtil.Builder fieldMappingUtilBuilder;


  /**
   * A list of fields that are left in the index for legacy reasons, but are removed from the public
   * API to avoid confusion.
   */
  @Value(
          "${index.fields.tohide:collector_text,location_determined,row_key,matched_name,decimal_latitudelatitude,collectors,default_values_used,generalisation_to_apply_in_metres,geohash,ibra_subregion,identifier_by,occurrence_details,text,photo_page_url,photographer,places,portal_id,quad,rem_text,occurrence_status_s,identification_qualifier_s}")
  protected String indexFieldsToHide;

  protected Pattern layersPattern = Pattern.compile("(el|cl)[0-9abc]+");

  @Inject
  protected QueryFormatUtils queryFormatUtils;

  @Value("${dwc.url:http://rs.tdwg.org/dwc/terms/}")
  protected String dwcUrl = "http://rs.tdwg.org/dwc/terms/";

  @Value("${solr.usehttp2:false}")
  protected Boolean usehttp2;

  /**
   * solr connection retry limit
   */
  @Value("${solr.server.retry.max:6}")
  protected int maxRetries = 6;
  /**
   * solr connection wait time between retries in ms
   */
  @Value("${solr.server.retry.wait:50}")
  protected long retryWait = 50;

  @Value("${solr.collection:biocache}")
  protected String solrCollection;

  @Value("${solr.connection.pool.size:50}")
  protected Integer solrConnectionPoolSize;

  @Value("${solr.connection.maxperroute:50}")
  protected Integer solrConnectionMaxPerRoute;

  @Value("${solr.connection.connecttimeout:30000}")
  private Integer solrConnectionConnectTimeout;

  @Value("${solr.connection.requesttimeout:30000}")
  private Integer solrConnectionRequestTimeout;

  @Value("${solr.connection.sockettimeout:30000}")
  private Integer solrConnectionSocketTimeout;

  @Value("${solr.connection.cache.entries:500}")
  private Integer solrConnectionCacheEntries;

  // 1024 * 256 = 262144 bytes
  @Value("${solr.connection.cache.object.size:262144}")
  private Integer solrConnectionCacheObjectSize;

  @Value("${biocache.useragent:Biocache}")
  private String userAgent;

  @Value("${solr.update.threads:4}")
  Integer solrUpdateThreads;

  @Value("${solr.batch.size:1000}")
  Integer solrBatchSize;

  @Value("${solr.legacyFieldNameSupport:true}")
  Boolean legacyFieldNameSupport;

  /**
   * solr index version refresh time in ms, 5*60*1000
   */
  @Value("${solr.server.indexVersion.refresh:300000}")
  protected int solrIndexVersionRefreshTime = 300000;

  @Value("${solr.home:}")
  protected String solrHome;

  // CoreContainer cc;
  SolrClient solrClient;
  CloseableHttpClient httpClient;
  HttpClientConnectionManager connectionPoolManager;

  @PostConstruct
  public void init() {
    if (solrClient == null) {
      if (usehttp2) {
        // TODO - this is experimental. Requires more configuration params for tuning timeouts etc
        if (!solrHome.startsWith("http")) {
          String[] zkHosts = solrHome.split(",");
          List<String> hosts = new ArrayList<String>();
          for (String zkHost : zkHosts) {
            hosts.add(zkHost.trim());
          }
          // HTTP2
          CloudHttp2SolrClient.Builder builder =
                  new CloudHttp2SolrClient.Builder(hosts, Optional.empty());
          CloudHttp2SolrClient client = builder.build();
          client.setDefaultCollection(solrCollection);
          solrClient = client;
        } else {
          Http2SolrClient.Builder builder = new Http2SolrClient.Builder(solrHome);
          builder.connectionTimeout(solrConnectionConnectTimeout);
          builder.maxConnectionsPerHost(solrConnectionMaxPerRoute);
          solrClient = builder.build();
        }
      }
      else {
        logger.info("Initialising the solr server " + solrHome);
        PoolingHttpClientConnectionManager poolingConnectionPoolManager =
            new PoolingHttpClientConnectionManager();
        poolingConnectionPoolManager.setMaxTotal(solrConnectionPoolSize);
        poolingConnectionPoolManager.setDefaultMaxPerRoute(solrConnectionMaxPerRoute);

        CacheConfig cacheConfig =
            CacheConfig.custom()
                .setMaxCacheEntries(solrConnectionCacheEntries)
                .setMaxObjectSize(solrConnectionCacheObjectSize)
                .setSharedCache(false)
                .build();
        RequestConfig requestConfig =
            RequestConfig.custom()
                .setConnectTimeout(solrConnectionConnectTimeout)
                .setConnectionRequestTimeout(solrConnectionRequestTimeout)
                .setSocketTimeout(solrConnectionSocketTimeout)
                .build();
        httpClient =
            CachingHttpClientBuilder.create()
                .setCacheConfig(cacheConfig)
                .setDefaultRequestConfig(requestConfig)
                .setConnectionManager(connectionPoolManager)
                .setUserAgent(userAgent)
                .useSystemProperties()
                .build();

        if (!solrHome.startsWith("http://")) {
          if (solrHome.contains(":")) {
            // assume that it represents a SolrCloud using ZooKeeper
            CloudSolrClient cloudServer =
                new CloudSolrClient.Builder()
                    .withZkHost(solrHome)
                    .withHttpClient(httpClient)
                    .build();
            cloudServer.setDefaultCollection(solrCollection);
            solrClient = cloudServer;
            try {
              solrClient.ping();
            } catch (Exception e) {
              logger.error("ping failed", e);
            }
          }
        } else {
          logger.info("Initialising connection to SOLR server..... with solrHome:  " + solrHome);
          solrClient =
              new ConcurrentUpdateSolrClient.Builder(solrHome)
                  .withThreadCount(solrUpdateThreads)
                  .withQueueSize(solrBatchSize)
                  .build();
          logger.info("Initialising connection to SOLR server - done.");
        }
      }
    }
  }

  @Override
  public void destroy() {
    try {
      // close SOLR connection
      solrClient.close();
    } catch (IOException e) {
      logger.error("failed to close solrClient", e);
    }
  }

  boolean legacyTranslation = true;

  @Override
  public QueryResponse query(SolrParams query) throws Exception {
    int retry = 0;

    QueryResponse qr = null;
    while (retry < maxRetries && qr == null) {
      retry++;
      try {
        if (logger.isDebugEnabled()) {
          logger.debug("SOLR query:" + query.toString());
        }

        qr = solrClient.query(query, SolrRequest.METHOD.POST); // can throw exception
      } catch (SolrServerException e) {
        // want to retry IOException and Proxy Error
        if (retry < maxRetries
                && (e.getMessage().contains("IOException") || e.getMessage().contains("Proxy Error"))) {
          if (retryWait > 0) {
            try {
              Thread.sleep(retryWait);
            } catch (InterruptedException ex) {
              // If the Thread sleep is interrupted, we shouldn't attempt to continue
              Thread.currentThread().interrupt();
              throw e;
            }
          }
        } else {
          // throw all other errors
          throw e;
        }
      } catch (SolrException e) {
        // Fix zk disconnects, maybe
        if (solrClient instanceof CloudSolrClient
                && e.getMessage().contains("Could not load collection")) {
          logger.error(
                  "query failed, attempting to reconnect: "
                          + query.toString()
                          + " : "
                          + e.getMessage());

          // zk reconnect
          try {
            ((CloudSolrClient) solrClient).getClusterStateProvider().close();
          } catch (IOException io) {
          }
          ((CloudSolrClient) solrClient).getClusterStateProvider().connect();

          // solr reconnect
          try {
            solrClient.close();
          } catch (IOException io) {
          }
          ((CloudSolrClient) solrClient).connect();

          if (retry < maxRetries) {
            if (retryWait > 0) {
              try {
                Thread.sleep(retryWait);
              } catch (InterruptedException ex) {
                // If the Thread sleep is interrupted, we shouldn't attempt to continue
                Thread.currentThread().interrupt();
                throw e;
              }
            }
          } else {
            throw e;
          }
        } else {
          logger.error("query failed: " + query.toString() + " : " + e.getMessage());
          throw e;
        }

      } catch (IOException ioe) {
        // report failed query
        logger.error("query failed: " + query.toString() + " : " + ioe.getMessage());
        throw new SolrServerException(ioe);
      } catch (Exception ioe) {
        // report failed query
        logger.error("query failed: " + query.toString() + " : " + ioe.getMessage());
        throw new SolrServerException(ioe);
      }
    }

    return qr;
  }

  /**
   * parses the response string from the service that returns details about the indexed fields
   *
   * @param str
   * @return
   */
  private Set<IndexFieldDTO> parseLukeResponse(String str, boolean includeCounts) {

    // update index version
    Pattern indexVersion = Pattern.compile("(?:version=)([0-9]{1,})");
    try {
      Matcher indexVersionMatcher = indexVersion.matcher(str);
      if (indexVersionMatcher.find(0)) {
        solrIndexVersion = Long.parseLong(indexVersionMatcher.group(1));
        solrIndexVersionTime = System.currentTimeMillis();
      }
    } catch (Exception e) {
    }

    Set<IndexFieldDTO> fieldList =
            includeCounts
                    ? new java.util.LinkedHashSet<IndexFieldDTO>()
                    : new java.util.TreeSet<IndexFieldDTO>();

    Pattern typePattern = Pattern.compile("(?:type=)([a-z]{1,})");

    Pattern schemaPattern = Pattern.compile("(?:schema=)([a-zA-Z\\-]{1,})");

    Pattern distinctPattern = Pattern.compile("(?:distinct=)([0-9]{1,})");

    String[] fieldsStr = str.split("fields=\\{");

    Map<String, String> indexToJsonMap = new OccurrenceIndex().indexToJsonMap();

    for (String fieldStr : fieldsStr) {
      if (fieldStr != null && !"".equals(fieldStr)) {
        String[] fields = includeCounts ? fieldStr.split("\\}\\},") : fieldStr.split("\\},");

        // sort fields for later use of indexOf
        Arrays.sort(fields);

        for (String field : fields) {
          formatIndexField(
                  field, null, fieldList, typePattern, schemaPattern, indexToJsonMap, distinctPattern);
        }
      }
    }

    fieldMappingUtilBuilder.getFieldMappingStream()
            .forEach((Map.Entry<String, String> fieldMapping) -> {

              IndexFieldDTO deprecatedFields = new IndexFieldDTO();
              deprecatedFields.setName(fieldMapping.getKey());
              deprecatedFields.setDeprecated(true);
              if (fieldMapping.getValue() != null) {
                deprecatedFields.setNewFieldName(fieldMapping.getValue());
              }

              fieldList.add(deprecatedFields);
            });

    // filter fields, to hide deprecated terms
    List<String> toIgnore = new ArrayList<String>();
    Set<IndexFieldDTO> filteredFieldList = new HashSet<IndexFieldDTO>();
    if (indexFieldsToHide != null) {
      toIgnore = Arrays.asList(indexFieldsToHide.split(","));
    }
    for (IndexFieldDTO indexedField : fieldList) {
      if (!toIgnore.contains(indexedField.getName())) {
        filteredFieldList.add(indexedField);
      }
    }

    return filteredFieldList;
  }

  /**
   * Gets the details about the SOLR fields using the LukeRequestHandler: See
   * http://wiki.apache.org/solr/LukeRequestHandler for more information
   */
  @Override
  public Set<IndexFieldDTO> getIndexFieldDetails(String... fields) throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("qt", "/admin/luke");

    params.set("tr", "luke.xsl");
    if (fields != null) {
      params.set("fl", fields);
      params.set("numTerms", "1");
    } else {
      // TODO: We should be caching the result locally without calling Solr in this case, as it is
      // called very often
      params.set("numTerms", "0");
    }
    QueryResponse response = query(params);
    return parseLukeResponse(response.toString(), fields != null);
  }

  @Override
  /**
   * Get the SOLR index version. Trigger a background refresh on a timeout.
   *
   * <p>Forcing an updated value will perform a new SOLR query for each request to be run in the
   * foreground.
   *
   * @param force
   * @return
   */
  public Long getIndexVersion(Boolean force) {
    Thread t = null;
    synchronized (solrIndexVersionLock) {
      boolean immediately = solrIndexVersionTime == 0;

      if (force
              || solrIndexVersionTime < System.currentTimeMillis() - solrIndexVersionRefreshTime) {
        solrIndexVersionTime = System.currentTimeMillis();

        t =
                new Thread() {
                  @Override
                  public void run() {
                    try {
                      getIndexFieldDetails(null);
                    } catch (Exception e) {
                      logger.error("Failed to update solrIndexVersion", e);
                    }
                  }
                };

        if (immediately) {
          // wait with lock
          t.start();
          try {
            t.join();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Failed to update solrIndexVersion", e);
          }
        } else if (!force) {
          // run in background
          t.start();
        }
      }
    }

    if (force && t != null) {
      // wait without lock
      t.start();
      try {
        t.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("Failed to update solrIndexVersion", e);
      }
    }

    return solrIndexVersion;
  }

  private final Map<String, StatsIndexFieldDTO> rangeFieldCache =
          new HashMap<String, StatsIndexFieldDTO>();

  /**
   * Obtains the Statistics for the supplied field so it can be used to determine the ranges.
   *
   * @param field
   * @return
   */
  public StatsIndexFieldDTO getRangeFieldDetails(String field) {
    StatsIndexFieldDTO details = rangeFieldCache.get(field);
    Map<String, IndexFieldDTO> nextIndexFieldMap = indexFieldMap;
    if (details == null && nextIndexFieldMap != null) {
      // get the details
      SpatialSearchRequestParams searchParams = new SpatialSearchRequestParams();
      searchParams.setQ("*:*");
      searchParams.setFacets(new String[]{field});
      try {
        Map<String, FieldStatsInfo> stats = getStatistics(searchParams);
        if (stats != null) {
          IndexFieldDTO ifdto = nextIndexFieldMap.get(field);
          if (ifdto != null) {
            String type = ifdto.getDataType();
            details = new StatsIndexFieldDTO(stats.get(field), type);
            rangeFieldCache.put(field, details);
          } else {
            if (logger.isDebugEnabled()) {
              logger.debug("Unable to locate field:  " + field);
            }
            return null;
          }
        }
      } catch (Exception e) {
        logger.warn("Unable to obtain range from cache.", e);
        details = null;
      }
    }

    return details;
  }

  private volatile Set<IndexFieldDTO> indexFields =
          new ConcurrentHashSet<
                  IndexFieldDTO>(); // RestartDataService.get(this, "indexFields", new
  // TypeReference<TreeSet<IndexFieldDTO>>(){}, TreeSet.class);
  private volatile Map<String, IndexFieldDTO> indexFieldMap =
          RestartDataService.get(
                  this,
                  "indexFieldMap",
                  new TypeReference<HashMap<String, IndexFieldDTO>>() {
                  },
                  HashMap.class);

  private void formatIndexField(
          String indexField,
          String cassandraField,
          Set<IndexFieldDTO> fieldList,
          Pattern typePattern,
          Pattern schemaPattern,
          Map indexToJsonMap,
          Pattern distinctPattern) {

    if (indexField != null && !"".equals(indexField)) {
      IndexFieldDTO f = new IndexFieldDTO();

      String fieldName = indexField.split("=")[0];
      String type = null;
      String schema = null;
      Matcher typeMatcher = typePattern.matcher(indexField);
      if (typeMatcher.find(0)) {
        type = typeMatcher.group(1);
      }

      Matcher schemaMatcher = schemaPattern.matcher(indexField);
      if (schemaMatcher.find(0)) {
        schema = schemaMatcher.group(1);
      }

      // don't allow the sensitive coordinates to be exposed via ws and don't allow index fields
      // without schema
      if (StringUtils.isNotEmpty(fieldName)
              && !fieldName.startsWith("sensitive_")
              && (cassandraField != null || schema != null)) {

        f.setName(fieldName);
        if (type != null) f.setDataType(type);
        else f.setDataType("string");

        // interpret the schema information
        if (schema != null) {
          f.setIndexed(schema.contains("I"));
          f.setStored(schema.contains("S"));
          f.setMultivalue(schema.contains("M"));
          f.setDocvalue(schema.contains("D"));
        }

        // now add the i18n and associated strings to the field.
        // 1. description: display name from fieldName= in i18n
        // 2. info: details about this field from description.fieldName= in i18n
        // 3. dwcTerm: DwC field name for this field from dwc.fieldName= in i18n
        // 4. jsonName: json key as returned by occurrences/search
        // 5. downloadField: biocache-store column name that is usable in DownloadRequestParams.fl
        // if the field has (5) downloadField, use it to find missing (1), (2) or (3)
        // 6. downloadDescription: the column name when downloadField is used in
        //   DownloadRequestParams.fl and a translation occurs
        // 7. i18nValues: true | false, indicates that the values returned by this field can be
        //   translated using facetName.value= in /facets/i18n
        // 8. class value for this field
        if (layersPattern.matcher(fieldName).matches()) {
          f.setDownloadName(fieldName);
          String description = layersService.getLayerNameMap().get(fieldName);
          f.setDescription(description);
          f.setDownloadDescription(description);
          f.setInfo(layersService.getLayersServiceUrl() + "/layers/view/more/" + fieldName);
          if (fieldName.startsWith("el")) {
            f.setClasss("Environmental");
          } else {
            f.setClasss("Contextual");
          }
        } else {
          // (5) check as a downloadField
          String downloadField = fieldName;
          if (cassandraField != null) {
            downloadField = cassandraField;
          }
          if (downloadField != null) {
            f.setDownloadName(downloadField);
          }

          // (6) downloadField description
          String downloadFieldDescription =
                  messageSource.getMessage(downloadField, null, "", Locale.getDefault());
          if (downloadFieldDescription.length() > 0) {
            f.setDownloadDescription(downloadFieldDescription);
            f.setDescription(downloadFieldDescription); // (1)
          }

          // (1) check as a field name
          String description =
                  messageSource.getMessage("facet." + fieldName, null, "", Locale.getDefault());
          if (description.length() > 0) {
            f.setDescription(description);
          } else if (downloadField != null) {
            description = messageSource.getMessage(downloadField, null, "", Locale.getDefault());
            if (description.length() > 0) {
              f.setDescription(description);
            }
          }

          // (2) check as a description
          String info =
                  messageSource.getMessage("description." + fieldName, null, "", Locale.getDefault());
          if (info.length() > 0) {
            f.setInfo(info);
          } else if (downloadField != null) {
            info =
                    messageSource.getMessage(
                            "description." + downloadField, null, "", Locale.getDefault());
            if (info.length() > 0) {
              f.setInfo(info);
            }
          }

          // (3) check as a dwcTerm
          String camelCase = LOWER_UNDERSCORE.to(LOWER_CAMEL, fieldName);

          Term term = null;
          try {
            // find matching Darwin core term
            term = DwcTerm.valueOf(camelCase);
          } catch (IllegalArgumentException e) {
            // enum not found
          }
          boolean dcterm = false;
          try {
            // find matching Dublin core terms that are not in miscProperties
            // include case fix for rightsHolder
            term = DcTerm.valueOf(camelCase.replaceAll("rightsholder", "rightsHolder"));
            dcterm = true;
          } catch (IllegalArgumentException e) {
            // enum not found
          }
          if (term == null) {
            // look in message properties. This is for irregular fieldName to DwcTerm matches
            String dwcTerm =
                    messageSource.getMessage("dwc." + fieldName, null, "", Locale.getDefault());
            if (downloadField != null) {
              dwcTerm =
                      messageSource.getMessage("dwc." + downloadField, null, "", Locale.getDefault());
            }

            if (dwcTerm.length() > 0) {
              f.setDwcTerm(dwcTerm);

              try {
                // find the term now
                term = DwcTerm.valueOf(dwcTerm);
                if (term != null) {
                  f.setClasss(((DwcTerm) term).getGroup()); // (8)
                }
                DwcTermDetails dwcTermDetails =
                        DwCTerms.getInstance().getDwCTermDetails(term.simpleName());
                if (dwcTermDetails != null) {
                  if (f.getInfo() == null) f.setInfo(dwcTermDetails.comment);
                  if (f.getDescription() == null) f.setDescription(dwcTermDetails.label);
                }
              } catch (IllegalArgumentException e) {
                // enum not found
              }
            }
          } else {

            f.setDwcTerm(term.simpleName());
            if (term instanceof DwcTerm) {
              f.setClasss(((DwcTerm) term).getGroup()); // (8)
            } else {
              f.setClasss(DwcTerm.GROUP_RECORD); // Assign dcterms to the Record group.
              // apply dcterm: prefix
              f.setDwcTerm("dcterms:" + term.simpleName());
            }

            DwcTermDetails dwcTermDetails =
                    DwCTerms.getInstance().getDwCTermDetails(term.simpleName());
            if (dwcTermDetails != null) {
              if (f.getInfo() == null) f.setInfo(dwcTermDetails.comment);
              if (f.getDescription() == null) f.setDescription(dwcTermDetails.label);
            }
          }

          // append dwc url to info
          if (!dcterm
                  && f.getDwcTerm() != null
                  && !f.getDwcTerm().isEmpty()
                  && StringUtils.isNotEmpty(dwcUrl)) {
            if (info.length() > 0) info += " ";
            f.setInfo(info + dwcUrl + f.getDwcTerm());
          }

          // (4) check as json name
          String json = (String) indexToJsonMap.get(fieldName);
          if (json != null) {
            f.setJsonName(json);
          }

          // (7) has lookupValues in i18n
          String i18nValues =
                  messageSource.getMessage("i18nvalues." + fieldName, null, "", Locale.getDefault());
          if (i18nValues.length() > 0) {
            f.setI18nValues("true".equalsIgnoreCase(i18nValues));
          }

          // (8) get class. This will override any DwcTerm.group
          String classs =
                  messageSource.getMessage("class." + fieldName, null, "", Locale.getDefault());
          if (classs.length() > 0) {
            f.setClasss(classs);
          }
        }

        fieldList.add(f);
      }

      Matcher distinctMatcher = distinctPattern.matcher(indexField);
      if (distinctMatcher.find(0)) {
        Integer distinct = Integer.parseInt(distinctMatcher.group(1));
        f.setNumberDistinctValues(distinct);
      }
    }
  }

  @Inject
  protected AbstractMessageSource messageSource;

  // empties the range cache to allow the settings to be recalculated.
  //        rangeFieldCache.clear();

  @Override
  public Set<IndexFieldDTO> getIndexedFields() throws Exception {
    return getIndexedFields(false);
  }

  /**
   * Returns details about the fields in the index.
   */
  // @Cacheable(cacheName = "getIndexedFields")
  public Set<IndexFieldDTO> getIndexedFields(boolean update) throws Exception {
    Set<IndexFieldDTO> result = indexFields;
    if (result.size() == 0 || update) {
      synchronized (solrIndexVersionLock) {
        result = indexFields;
        if (result.size() == 0 || update) {
          result = getIndexFieldDetails(null);
          if (result != null && result.size() > 0) {
            Map<String, IndexFieldDTO> resultMap = new HashMap<String, IndexFieldDTO>();
            for (IndexFieldDTO field : result) {
              resultMap.put(field.getName(), field);
            }
            indexFields = result;
            indexFieldMap = resultMap;
          }
        }
      }
    }
    return result;
  }

  @Override
  public Map<String, IndexFieldDTO> getIndexedFieldsMap() throws Exception {
    // Refresh/populate the map if necessary
    getIndexedFields();
    return indexFieldMap;
  }

  /**
   * @see au.org.ala.biocache.dao.SearchDAO#getStatistics(SpatialSearchRequestParams)
   */
  @Override
  public Map<String, FieldStatsInfo> getStatistics(SpatialSearchRequestParams searchParams)
          throws Exception {
    try {
      queryFormatUtils.formatSearchQuery(searchParams);
      String queryString = searchParams.getFormattedQuery();
      SolrQuery solrQuery = new SolrQuery();
      solrQuery.setQuery(queryString);
      for (String field : searchParams.getFacets()) {
        solrQuery.setGetFieldStatistics(field);
      }
      QueryResponse qr = runSolrQuery(solrQuery, searchParams);
      if (logger.isDebugEnabled()) {
        logger.debug(qr.getFieldStatsInfo());
      }
      return qr.getFieldStatsInfo();

    } catch (SolrServerException ex) {
      logger.error("Problem communicating with SOLR server. " + ex.getMessage(), ex);
    }
    return null;
  }

  /**
   * Perform SOLR query - takes a SolrQuery and search params
   *
   * @param solrQuery
   * @param requestParams
   * @return
   * @throws SolrServerException
   */
  @Override
  public QueryResponse runSolrQuery(SolrQuery solrQuery, SearchRequestParams requestParams)
          throws Exception {

    if (requestParams.getFormattedFq() != null) {
      for (String fq : requestParams.getFormattedFq()) {
        if (StringUtils.isNotEmpty(fq)) {
          solrQuery.addFilterQuery(fq);
        }
      }
    }

    // include null facets
    solrQuery.setFacetMissing(true);
    solrQuery.setRows(requestParams.getPageSize());
    solrQuery.setStart(requestParams.getStart());
    if (StringUtils.isNotEmpty(requestParams.getDir())) {
      solrQuery.setSort(requestParams.getSort(), SolrQuery.ORDER.valueOf(requestParams.getDir()));
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Solr query: " + solrQuery.toString());
    }
    QueryResponse qr = query(solrQuery); // can throw exception
    if (logger.isDebugEnabled()) {
      logger.debug("qtime:" + qr.getQTime());
      if (qr.getResults() == null) {
        logger.debug("no results");
      } else {
        logger.debug("Matched records: " + qr.getResults().getNumFound());
      }
    }
    return qr;
  }
}

class FacetFieldRenamed extends FacetField {

  FacetField facetField;

  public FacetFieldRenamed(String n) {
    super(n);
  }

  public FacetFieldRenamed(String name, FacetField facetField) {
    super(name);
    this.facetField = facetField;
  }

  public void add(String name, long cnt) {
    facetField.add(name, cnt);
  }

  public void insert(String name, long cnt) {
    facetField.insert(name, cnt);
  }

  //    public String getName() {
  //        return this.getName();
  //    }

  public List<FacetField.Count> getValues() {
    return facetField.getValues();
  }

  public int getValueCount() {
    return facetField.getValueCount();
  }

  public FacetField getLimitingFields(long max) {
    return facetField.getLimitingFields(max);
  }

  public String toString() {
    return getName() + ":" + facetField.getValues();
  }
}

class RangeFacetRenamed<B, G> extends RangeFacet<B, G> {

  protected RangeFacetRenamed(
          String name, B start, B end, G gap, Number before, Number after, Number between) {
    super(name, start, end, gap, before, after, between);
  }

  public RangeFacetRenamed(String name, RangeFacet<B, G> rangedFacet) {
    super(
            name,
            rangedFacet.getStart(),
            rangedFacet.getEnd(),
            rangedFacet.getGap(),
            rangedFacet.getBefore(),
            rangedFacet.getAfter(),
            rangedFacet.getBetween());
  }
}
