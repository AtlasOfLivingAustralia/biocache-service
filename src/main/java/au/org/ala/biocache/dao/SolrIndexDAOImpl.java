package au.org.ala.biocache.dao;

import au.org.ala.biocache.dto.AssertionStatus;
import au.org.ala.biocache.dto.IndexFieldDTO;
import au.org.ala.biocache.dto.OccurrenceIndex;
import au.org.ala.biocache.service.LayersService;
import au.org.ala.biocache.service.RestartDataService;
import au.org.ala.biocache.stream.ProcessInterface;
import au.org.ala.biocache.util.DwCTerms;
import au.org.ala.biocache.util.DwcTermDetails;
import au.org.ala.biocache.util.solr.FieldMappedSolrClient;
import au.org.ala.biocache.util.solr.FieldMappingUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.cache.CacheConfig;
import org.apache.http.impl.client.cache.CachingHttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.*;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.*;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.support.AbstractMessageSource;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
    private FieldMappingUtil fieldMappingUtil;

    /*
     * The csv header for updating solr
     */
    private final List<String> header = Arrays.asList(
            "userAssertions",
            "hasUserAssertions",
            "userVerified",
            "lastAssertionDate"
    );

    /*
     * The field types to exclude from the index/fields JSON
     */
    @Value("${index.fields.skipFieldTypes:location,geohash,quad,packedQuad}")
    private final List<String> skipFieldTypes = new ArrayList<>();

    /*
     * This structure holds field properties
     * Values in the list are:
     * String fieldType, Boolean multiValued, Boolean docValues, Boolean indexed, Boolean stored
     */
    private final Map<String, Object[]> fieldProperties = new HashMap<String, Object[]>() {
        {
            put("assertionUserId", new Object[]{"string", true, true, true, true});
            put("userAssertions", new Object[]{"string", false, true, true, true});
            put("hasUserAssertions", new Object[]{"boolean", false, true, true, true});
            put("lastAssertionDate", new Object[]{"date", false, true, true, true});
            put("userVerified", new Object[]{"boolean", false, true, true, true});
            put("_version_", new Object[]{"long", false, true, false, false});
        }
    };

    /**
     * A list of fields that are left in the index for legacy reasons, but are removed from the public
     * API to avoid confusion.
     */
    @Value("${index.fields.tohide:_version_}")
    protected String indexFieldsToHide;

    protected Pattern layersPattern = Pattern.compile("(el|cl)[0-9abc]+");

    @Value("${dwc.url:http://rs.tdwg.org/dwc/terms/}")
    protected String dwcUrl = "http://rs.tdwg.org/dwc/terms/";

    @Value("${solr.usehttp2:false}")
    protected Boolean usehttp2;

    /**
     * solr connection retry limit
     */
    @Value("${solr.server.retry.max:1}")
    protected int maxRetries = 1;
    /**
     * solr connection wait time between retries in ms
     */
    @Value("${solr.server.retry.wait:1000}")
    protected long retryWait = 1000;

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

    // for SOLR streaming
    SolrClientCache solrClientCache;

    @PostConstruct
    public void init() {

        if (solrClient == null) {

            SolrClient solrClient = null;

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
                            .setConnectionManager(poolingConnectionPoolManager)
                            .setMaxConnPerRoute(solrConnectionMaxPerRoute)
                            .setUserAgent(userAgent)
                            .useSystemProperties()
                            .build();

            solrClientCache = new SolrClientCache();

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
            } else {
                logger.info("Initialising the solr server " + solrHome);

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
                    } else {
                        logger.error("Failed to initialise connection to SOLR server with solrHome: " + solrHome);
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

            if (solrClient != null) {
                this.solrClient = new FieldMappedSolrClient(fieldMappingUtil, solrClient);
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
                    logError(query, "query failed, attempting to reconnect: ", e.getMessage());

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
                    logError(query, "query failed-1", e.getMessage());
                    throw e;
                }

            } catch (IOException ioe) {
                // report failed query
                logError(query, "query failed-IOException ", ioe.getMessage());
                throw new SolrServerException(ioe);
            } catch (Exception ioe) {
                // report failed query
                logError(query, "query failed-SolrServerException ", ioe.getMessage());
                throw new SolrServerException(ioe);
            }
        }

        return qr;
    }

    private void logError(SolrParams query, String message, String exceptionMessage) {
        String requestID = MDC.get("X-Request-ID");
        if (requestID != null) {
            logger.error("RequestID:" + requestID + ", " + message + " - SOLRQuery: " + query.toString() + ", Error : " + exceptionMessage);
        } else {
            logger.error(message + " - SOLRQuery: " + query.toString() + " : " + exceptionMessage);
        }
    }

    /**
     * parses the response string from the service that returns details about the indexed fields
     *
     * @param qr
     * @return
     */
    private void parseLukeResponse(QueryResponse qr, Map<String, IndexFieldDTO> indexFieldMap) {

        NamedList response = qr.getResponse();

        solrIndexVersion = (long) ((NamedList) response.get("index")).get("version");
        solrIndexVersionTime = System.currentTimeMillis();

        Map<String, String> indexToJsonMap = new OccurrenceIndex().indexToJsonMap();

        NamedList<NamedList<Object>> fields = (NamedList) response.get("fields");
        fields.forEach((String fieldName, NamedList<Object> fieldInfo) -> {

            if (StringUtils.isNotEmpty(fieldName) && fieldInfo != null) {

                IndexFieldDTO indexField = indexFieldMap.get(fieldName);

                if (indexField == null) {

                    String fieldType = fieldInfo.get("type").toString();
                    String schema = fieldInfo.get("schema").toString();
                    Integer distinctCount = (Integer) fieldInfo.get("distinct");

                    indexField = formatIndexField(fieldName, fieldType, schema, distinctCount, indexToJsonMap);

                    if (indexField != null) {
                        indexFieldMap.put(fieldName, indexField);
                    }

                } else {

                    indexField.setNumberDistinctValues((Integer) fieldInfo.get("distinct"));
                }
            }
        });
    }

    private void parseSchemaResponse(QueryResponse qr, Map<String, IndexFieldDTO> indexFieldMap) {

        Map<String, String> indexToJsonMap = new OccurrenceIndex().indexToJsonMap();

        NamedList response = qr.getResponse();

        NamedList<NamedList<Object>> fields = (NamedList) ((NamedList) response.get("schema")).get("fields");

        fields.forEach((String fieldName, NamedList<Object> fieldInfo) -> {

            if (StringUtils.isNotEmpty(fieldName) && fieldInfo != null) {

                IndexFieldDTO indexField = indexFieldMap.get(fieldName);

                if (indexField == null) {

                    String fieldType = fieldInfo.get("type").toString();
                    String schema = fieldInfo.get("flags").toString();

                    indexField = formatIndexField(fieldName, fieldType, schema, null, indexToJsonMap);

                    if (indexField != null) {
                        indexFieldMap.put(fieldName, indexField);
                    }
                }

                ArrayList copySources = (ArrayList) fieldInfo.get("copySources");

                if (indexField != null && copySources != null && copySources.size() > 0) {

                    indexField.setSourceFields(copySources);
                }
            }
        });
    }

    /**
     * Gets the details about the SOLR fields using the LukeRequestHandler: See
     * http://wiki.apache.org/solr/LukeRequestHandler for more information
     * @return
     */
    @Override
    public Set<IndexFieldDTO> getIndexFieldDetails(String... fields) throws Exception {
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("qt", "/admin/luke");

        params.set("tr", "luke.xsl");
        if (fields != null) {
            params.set("fl", String.join(",", fields));
            params.set("numTerms", "1");
        } else {
            // TODO: We should be caching the result locally without calling Solr in this case, as it is
            // called very often
            params.set("numTerms", "0");
        }
        QueryResponse response = query(params);

        Map<String, IndexFieldDTO> indexFieldMap = new java.util.Hashtable<>();
//        Set<IndexFieldDTO> indexFields =
//                fields != null
//                        ? new java.util.LinkedHashSet<IndexFieldDTO>()
//                        : new java.util.TreeSet<IndexFieldDTO>();

        parseLukeResponse(response, indexFieldMap);

        params = new ModifiableSolrParams();
        params.set("qt", "/admin/luke");
        params.set("show", "schema");

        response = query(params);

        parseSchemaResponse(response, indexFieldMap);

        fieldMappingUtil.getFieldMappingStream()
                .forEach((Pair<String, String> fieldMapping) -> {

                    IndexFieldDTO deprecatedFields = new IndexFieldDTO();
                    deprecatedFields.setName(fieldMapping.getKey());
                    deprecatedFields.setDeprecated(true);
                    if (fieldMapping.getValue() != null) {
                        deprecatedFields.setNewFieldName(fieldMapping.getValue());
                    }

                    indexFieldMap.put(deprecatedFields.getName(), deprecatedFields);
                });

        for (String indexFieldToHide: indexFieldsToHide.split(",")) {
            indexFieldMap.remove(indexFieldToHide);
        }

        // Insert entries for cl* and el* items
        Map<String, String> layerNameMap = layersService.getLayerNameMap();
        for (Map.Entry<String, String> item : layerNameMap.entrySet()) {
            IndexFieldDTO field = new IndexFieldDTO();
            field.setDocvalue(true);
            field.setIndexed(true);
            field.setStored(true);
            field.setDescription(item.getValue());
            field.setName(item.getKey());
            field.setDownloadName(item.getKey());
            if (item.getKey().startsWith("cl")) {
                field.setDataType("string");
            } else {
                field.setDataType("float");
            }
            indexFieldMap.put(item.getKey(), field);
        }

        if (fields != null && fields.length > 0) {
            return Arrays.stream(fields)
                    .map(fieldName -> indexFieldMap.get(fieldName))
                    .filter(indexField -> indexField != null)
                    .collect(Collectors.toSet());
        }

        return indexFieldMap.entrySet().stream().map(Map.Entry::getValue).collect(Collectors.toSet());
    }

    @Override
    public Set<String> getSchemaFields() throws Exception {

        return getSchemaFields(false);
    }

    /**
     * Returns details about the fields in the schema.
     */
    @Override
    public Set<String> getSchemaFields(boolean update) throws Exception {

        Set<String> result = schemaFields;

        if (result.size() == 0 || update) {

            synchronized (solrIndexVersionLock) {

                result = schemaFields;
                if (result.size() == 0 || update) {

                    ModifiableSolrParams params = new ModifiableSolrParams();
                    params.set("qt", "/admin/luke");
                    params.set("show", "schema");

                    QueryResponse response = query(params);

                    NamedList<Object> schemaFields = (NamedList) ((NamedList) response.getResponse().get("schema")).get("fields");

                    Iterator<Map.Entry<String, Object>>  iter = schemaFields.iterator();
                    result = new HashSet<>();
                    while (iter.hasNext()){
                        result.add(iter.next().getKey());
                    }

                    if (result != null && result.size() > 0) {
                        this.schemaFields = result;
                    }
                }
            }
        }
        return result;
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
                                    getIndexFieldDetails();
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

    private volatile Set<IndexFieldDTO> indexFields = new ConcurrentHashSet<
            IndexFieldDTO>();

    private volatile Map<String, IndexFieldDTO> indexFieldMap =
            RestartDataService.get(
                    this,
                    "indexFieldMap",
                    new TypeReference<HashMap<String, IndexFieldDTO>>() {
                    },
                    HashMap.class);

    private volatile Set<String> schemaFields = new HashSet();

    private IndexFieldDTO formatIndexField(String fieldName, String fieldType, String schema, Integer distinctCount, Map indexToJsonMap) {

        // don't allow the sensitive coordinates to be exposed via ws and don't allow index fields
        // without schema
        if (StringUtils.isNotEmpty(fieldName)
                && !fieldName.startsWith("sensitive_")
                && (schema != null)) {

            IndexFieldDTO f = new IndexFieldDTO();

            f.setName(fieldName);

            if ((fieldType != null && skipFieldTypes.contains(fieldType)) || fieldName.startsWith("_")) {
                // Skip fields we don't want to expose
                return null;
            } else {
                f.setDataType(Objects.requireNonNullElse(fieldType, "string"));
            }

            if (distinctCount != null) {
                f.setNumberDistinctValues(distinctCount);
            }

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
            // 5. downloadField: column name that is usable in DownloadRequestParams.fl (same as name)
            // if the field has (5) downloadField, use it to find missing (1), (2) or (3)
            // 6. downloadDescription: the column name when downloadField is used in
            //   DownloadRequestParams.fl and a translation occurs
            // 7. i18nValues: true | false, indicates that the values returned by this field can be
            //   translated using facetName.value= in /facets/i18n
            // 8. class value for this field
            // 9. infoUrl: wiki link from wiki.fieldName= in i18n
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
                if (downloadField != null) {
                    f.setDownloadName(downloadField);
                }

                fieldName = fieldMappingUtil.translateFieldName(fieldName);
                downloadField = fieldMappingUtil.translateFieldName(downloadField);

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
                Term term = null;
                try {
                    // find matching Darwin core term
                    term = DwcTerm.valueOf(fieldName);
                } catch (IllegalArgumentException e) {
                    // enum not found
                }
                boolean dcterm = false;
                try {
                    // find matching Dublin core terms that are not in miscProperties
                    // include case fix for rightsHolder
                    term = DcTerm.valueOf(fieldName);
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

                //(9) has wiki link in i18n
                String wikiLink = messageSource.getMessage("wiki." + fieldName, null, "", Locale.getDefault());
                if (wikiLink.length() > 0) {
                    f.setInfoUrl(wikiLink);
                }
            }

            return f;
        }

        return null;
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
    public Set<IndexFieldDTO> getIndexedFields(boolean update) throws Exception {
        Set<IndexFieldDTO> result = indexFields;
        if (result.size() == 0 || update) {
            synchronized (solrIndexVersionLock) {
                result = indexFields;
                if (result.size() == 0 || update) {
                    result = getIndexFieldDetails();
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
     * @see au.org.ala.biocache.dao.IndexDAO#getStatistics(String)
     */
    @Override
    public Map<String, FieldStatsInfo> getStatistics(String field)
            throws Exception {
        try {

            SolrQuery solrQuery = new SolrQuery();
            solrQuery.addGetFieldStatistics(field);
            solrQuery.setQuery("*:*");
            solrQuery.setRows(0);

            QueryResponse qr = runSolrQuery(solrQuery);
            if (logger.isDebugEnabled()) {
                logger.debug(qr.getFieldStatsInfo());
            }
            return qr.getFieldStatsInfo();

        } catch (SolrServerException ex) {

            String requestID = MDC.get("X-Request-ID");
            if (requestID != null) {
                logger.error("Problem communicating with SOLR server. RequestID:" + requestID + " Error:" + ex.getMessage(), ex);
            } else {
                logger.error("Problem communicating with SOLR server. Error:" + ex.getMessage(), ex);
            }
        }
        return null;
    }

    /**
     * Perform SOLR query - takes a SolrQuery and search params
     *
     * @param solrQuery
     * @return
     * @throws SolrServerException
     */
    @Override
    public QueryResponse runSolrQuery(SolrQuery solrQuery)
            throws Exception {

        // include null facets
        if (MDC.get("X-Request-ID") != null) {
            solrQuery.setParam("XRequestID", MDC.get("X-Request-ID"));
        }
        solrQuery.setFacetMissing(true);

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

    // read values mapping to csv headers
    private List<Object> getValues(Map<String, Object> map) {
        String userAssertionStatus = (String) map.getOrDefault("userAssertions", String.valueOf(AssertionStatus.QA_NONE));
        if (userAssertionStatus.equals(String.valueOf(AssertionStatus.QA_NONE))) {
            userAssertionStatus = null;
        }

        boolean hasUserAssertions = (boolean) map.getOrDefault("hasUserAssertions", false);
        boolean userVerified = (boolean) map.getOrDefault("userVerified", false);
        Date lastAssertionDate = (Date) map.getOrDefault("lastAssertionDate", null);

        return Arrays.asList(userAssertionStatus, hasUserAssertions, userVerified, lastAssertionDate);
    }

    @Override
    public void indexFromMap(List<Map<String, Object>> maps) throws IOException, SolrServerException {
        List<SolrInputDocument> batch = new ArrayList<>();
        for (Map<String, Object> map : maps) {
            if (map.containsKey("record_uuid")) {
                SolrInputDocument doc = new SolrInputDocument();
                doc.addField("id", map.get("record_uuid"));

                List<Object> values = getValues(map);
                if (values.size() > 0 && values.size() != header.size()) {
                    logger.error("Values don't match headers");
                    return;
                }

                for (int i = 0; i < header.size(); i++) {
                    String key = header.get(i);
                    Object value = values.get(i);
                    doc.addField(key, new HashMap<String, Object>() {{
                        put("set", value);
                    }});
                }

                // get list of user ids
                doc.addField("assertionUserId", new HashMap<String, Object>() {{
                    put("set", map.getOrDefault("assertionUserId", null));
                }});

                // this makes sure update only succeeds when record with specified id exists
                doc.addField("_version_", 1);

                // update schema if needed
                syncDocFieldsWithSOLR(doc);
                logger.debug("Added solr doc for record: " + doc.get("id") + " to updateRequest");
                batch.add(doc);
            }

            if (batch.size() == solrBatchSize) {
                updateBatch(batch);
                batch.clear();
            }
        }
        updateBatch(batch);
    }

    private void updateBatch(List<SolrInputDocument> batch) {
        if (!batch.isEmpty()) {
            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.setAction(UpdateRequest.ACTION.COMMIT, false, false);
            updateRequest.add(batch);
            logger.debug(batch.size() + " solr docs being updated");
            try {
                updateRequest.process(solrClient);
            } catch (Exception e) {
                logger.error("Failed to update solr doc, error message: " + e.getMessage(), e);
            }
        }
    }

    private void syncDocFieldsWithSOLR(SolrInputDocument doc) {
        doc.getFieldNames().forEach(fieldName -> {
            if (!schemaFields.contains(fieldName)) {
                Object[] properties = fieldProperties.get(fieldName);
                addFieldToSolr(fieldName, (String) properties[0], (Boolean) properties[1], (Boolean) properties[2], (Boolean) properties[3], (Boolean) properties[4]);
                schemaFields.add(fieldName);
            }
        });
    }

    private void addFieldToSolr(String name, String fieldType, Boolean multiValued, Boolean docValues, Boolean indexed, Boolean stored) {
        try {
            SchemaRequest.Field fieldRequest = new SchemaRequest.Field(name);
            fieldRequest.process(solrClient);
        } catch (Exception e) {
            logger.info("Field not in schema: " + name);
            Map<String, Object> field = new HashMap<>();
            field.put("name", name);
            field.put("type", fieldType);
            field.put("multiValued", multiValued);
            field.put("docValues", docValues);
            field.put("indexed", indexed);
            field.put("stored", stored);

            SchemaRequest.AddField addField = new SchemaRequest.AddField(field);
            try {
                logger.info("Adding field: " + name);
                addField.process(solrClient);
            } catch (Exception e1) {
                logger.error("Failed to add a new field '" + name + "' to SOLR schema. " + e1.getMessage());
            }
        }
    }

    private TupleStream openStream(SolrParams params) throws IOException {
        TupleStream solrStream = null;

        if (!solrHome.startsWith("http://")) {
            if (solrHome.contains(":")) {
                // assume that it represents a SolrCloud using ZooKeeper
                solrStream = new AlaCloudSolrStream(solrHome, solrCollection, params);
            } else {
                logger.error("Badly formatted solrHome configuration: " + solrHome);
                return null;
            }
        } else {
            // assume no ZooKeeper
            solrStream = new SolrStream(solrHome, params);
        }

        // Use solrClientCache to recycle SolrClients.
        StreamContext streamContext = new StreamContext();
        streamContext.setSolrClientCache(solrClientCache);
        solrStream.setStreamContext(streamContext);
        solrStream.open();

        return solrStream;
    }

    /**
     * Stream a solrQuery and apply proc.process to each tuple returned.
     *
     * @param query
     * @param procSearch
     * @param procFacet
     * @throws SolrServerException
     */
    @Override
    public int streamingQuery(SolrQuery query, ProcessInterface procSearch, ProcessInterface procFacet, SolrQuery endemicFacetSuperset) throws SolrServerException {
        int tupleCount = 0;
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("SOLR query:" + query.toString());
            }

            // do search
            if (procSearch != null && query.getRows() != 0) {
                try (TupleStream solrStream = openStream(buildSearchExpr(query));) {
                    Tuple tuple;
                    while (!(tuple = solrStream.read()).EOF && (tupleCount < query.getRows() || query.getRows() < 0)) {
                        tupleCount++;
                        procSearch.process(tuple);
                    }
                    procSearch.flush();
                }
            }

            // do facets
            if (procFacet != null && query.getFacetFields() != null) {
                // process one at a time
                for (String facetField : query.getFacetFields()) {
                    try (TupleStream solrStream = createTupleStream(query, endemicFacetSuperset, facetField);) {
                        Tuple tuple;
                        while (!(tuple = solrStream.read()).EOF) {
                            procFacet.process(tuple);
                        }
                    }
                }
                procFacet.flush();
            }
        } catch (HttpSolrClient.RemoteSolrException e) {
            logError(query, "SolrException query failed", e.getMessage());
            throw e;
        } catch (IOException ioe) {
            logError(query, "IOException query failed", ioe.getMessage());
            throw new SolrServerException(ioe);
        } catch (Exception ioe) {
            logError(query, "Exception - query failed", ioe.getMessage());
            throw new SolrServerException(ioe);
        }

        return tupleCount;
    }

    private TupleStream createTupleStream(SolrQuery query, SolrQuery endemicFacetSuperset, String facetField) throws IOException {
        if (endemicFacetSuperset == null) {
            return openStream(buildFacetExpr(query, facetField));
        } else {
            return openStream(buildEndemicExpr(query, endemicFacetSuperset));
        }
    }

    private ModifiableSolrParams buildSearchExpr(SolrQuery query) {
        ModifiableSolrParams solrParams = new ModifiableSolrParams();

        solrParams.set("q", fieldMappingUtil.translateQueryFields(query.getQuery()));

        if (query.getFilterQueries() != null) {
            for (String fq : query.getFilterQueries()) {
                if (StringUtils.isNotEmpty(fq)) {
                    solrParams.add("fq", fieldMappingUtil.translateQueryFields(fq));
                }
            }
        }

        if (StringUtils.isNotEmpty(query.getFields())) {
            solrParams.set("fl", StringUtils.join(fieldMappingUtil.translateFieldArray(query.getFields().split(",")), ","));
        } else {
            solrParams.set("fl", "id");
        }

        // The sort field is required, sometimes. Testing suggests that this depends on the solrClient class.
        // 1. use the query specified sort, without checking if it is in the list of fields
        // 2. use the first non-multivalue field
        // 3. use "id asc" and append "id" to the fl if it is missing
        //
        // Handle the error for some solrClient classes when the default query sort is "score asc" by excluding it
        if (StringUtils.isNotEmpty(query.getSortField()) && !"score asc".equals(query.getSortField())) {
            solrParams.set("sort", query.getSortField());
        } else {
            String [] fl = solrParams.get("fl").split(",");
            String nonMultivalueField = null;
            for (String field : fl) {
                IndexFieldDTO f = indexFieldMap.get(field);
                if (f != null && !f.isMultivalue()) {
                    nonMultivalueField = f.getName();
                    break;
                }
            }
            if (nonMultivalueField != null) {
                solrParams.set("sort", nonMultivalueField + " asc");
            } else {
                solrParams.set("sort", "id asc");

                // append "id" to field list
                solrParams.set("fl", solrParams.get("fl") + ",id");
            }
        }

        String qt = "/export";

        // Use /select handler when fewer than all rows are requested
        if (query.getStart() > 0 || query.getRows() > 0) {
            solrParams.set("rows", query.getRows());
            solrParams.set("start", query.getStart());
            qt = "/select";
        }
        solrParams.set("qt", qt);

        return solrParams;
    }

    private ModifiableSolrParams buildFacetExpr(SolrQuery query, String facetName) {
        StringBuilder cexpr = new StringBuilder();
        cexpr.append("facet(").append(solrCollection).append(", q=\"").append(escapeDoubleQuote(fieldMappingUtil.translateQueryFields(query.getQuery()))).append("\"");
        if (query.getFilterQueries() != null) {
            for (String fq : query.getFilterQueries()) {
                cexpr.append(", fq=\"").append(escapeDoubleQuote(fieldMappingUtil.translateQueryFields(fq))).append("\"");
            }
        }

        cexpr.append(", buckets=\"").append(fieldMappingUtil.translateFieldName(facetName)).append("\"");

        // translate the 'count' and 'index' facet.sort values for streaming facet()
        if (query.getFacetSortString() != null && query.getFacetSortString().endsWith(" asc") || query.getFacetSortString().endsWith(" desc")) {
            // use the streaming facet() compatible sort string
            cexpr.append(", bucketSorts=\"").append(query.getFacetSortString()).append("\"");
        } else if ("index".equals(query.getFacetSortString())) {
            cexpr.append(", bucketSorts=\"").append(facetName).append(" asc\"");
        } else {
            // default to count sort
            cexpr.append(", bucketSorts=\"count(*) desc\", count(*)");
        }

        int limit = query.getFacetLimit() >= 0 ? query.getFacetLimit() : -1;
        if (query.get("facet.offset") != null && limit != -1) {
            //cexpr.append(", offset=").append(query.get("facet.offset"));
            cexpr.append(", bucketSizeLimit=").append(limit + Integer.parseInt(query.get("facet.offset")));
        } else {
            cexpr.append(", bucketSizeLimit=").append(limit);
        }

        cexpr.append(")");

        String qt = "/stream";

        ModifiableSolrParams solrParams = new ModifiableSolrParams();
        solrParams.set("expr", cexpr.toString());
        solrParams.set("qt", qt);

        // declare that the request should be sent once, not to each shard
        solrParams.set("distrib", "true");

        return solrParams;
    }

    private ModifiableSolrParams buildEndemicExpr(SolrQuery subset, SolrQuery superset) {
        StringBuilder cexpr = new StringBuilder();

        String facetName = subset.getFacetFields()[0];

        String translatedFacetName = fieldMappingUtil.translateFieldName(facetName);

        // only include facetNames that ONLY appear in the target area
        cexpr.append("having(eq(count1, count(*))").
                // join target area facet counts with all areas facet counts
                        append(", innerJoin(on=\"").append(translatedFacetName).append("\"").

                // select target facet counts to rename count(*) as count1
                        append(", select(").
                // facet counts for target query
                        append("facet(").append(solrCollection).append(", q=\"").
                append(escapeDoubleQuote(fieldMappingUtil.translateQueryFields(subset.getQuery()))).append("\"");
        if (subset.getFilterQueries() != null) {
            for (String fq : subset.getFilterQueries()) {
                cexpr.append(", fq=\"").append(escapeDoubleQuote(fieldMappingUtil.translateQueryFields(fq))).append("\"");
            }
        }
        cexpr.append(", buckets=\"").append(translatedFacetName).append("\"").
                append(", bucketSorts=\"").append(translatedFacetName).append(" asc\"").
                append(", bucketSizeLimit=\"-1\")").    //close facet
                append(",").append(translatedFacetName).append(", count(*) as count1)").    //close select

                // facet counts for all records
                        append(", facet(").append(solrCollection).append(", q=\"").
                append(escapeDoubleQuote(fieldMappingUtil.translateQueryFields(superset.getQuery()))).append("\"");
        if (superset.getFilterQueries() != null) {
            for (String fq : superset.getFilterQueries()) {
                cexpr.append(", fq=\"").append(escapeDoubleQuote(fieldMappingUtil.translateQueryFields(fq))).append("\"");
            }
        }
        cexpr.append(", buckets=\"").append(translatedFacetName).append("\"").
                append(", bucketSorts=\"").append(translatedFacetName).append(" asc\"").
                append(", bucketSizeLimit=\"-1\")").    //close facet

                append(")"). // close innerJoin
                append(")"); // close having

        String qt = "/stream";

        ModifiableSolrParams solrParams = new ModifiableSolrParams();
        solrParams.set("expr", cexpr.toString());
        solrParams.set("qt", qt);

        // declare that the request should be sent once, not to each shard
        solrParams.set("distrib", "true");

        return solrParams;
    }

    private String escapeDoubleQuote(String input) {
        return input.replaceAll("\"", "\\\\\"");
    }
}
