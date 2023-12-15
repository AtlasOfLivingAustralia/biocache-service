/**************************************************************************
 *  Copyright (C) 2013 Atlas of Living Australia
 *  All Rights Reserved.
 *
 *  The contents of this file are subject to the Mozilla Public
 *  License Version 1.1 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://www.mozilla.org/MPL/
 *
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 ***************************************************************************/
package au.org.ala.biocache.service;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import au.org.ala.biocache.dao.IndexDAO;
import au.org.ala.biocache.dao.PersistentQueueDAO;
import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dto.*;
import au.org.ala.biocache.dto.DownloadDetailsDTO.DownloadType;
import au.org.ala.biocache.stream.OptionalZipOutputStream;
import au.org.ala.biocache.util.AlaFileUtils;
import au.org.ala.biocache.util.TooManyDownloadRequestsException;
import au.org.ala.biocache.writer.RecordWriterException;
import au.org.ala.doi.CreateDoiResponse;
import au.org.ala.ws.security.profile.AlaUserProfile;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.ala.client.model.LogEventVO;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.RuntimeServices;
import org.apache.velocity.runtime.RuntimeSingleton;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.support.AbstractMessageSource;
import org.springframework.core.io.Resource;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestOperations;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 * Services to perform the downloads.
 * <p>
 * Can configure the number of off-line download processors
 *
 * @author Natasha Carter (natasha.carter@csiro.au)
 */
@Component("downloadService")
public class DownloadService implements ApplicationListener<ContextClosedEvent> {

    public static final String OFFICIAL_DOI_RESOLVER = "https://doi.org/";
    public static final String CSDM_SELECTOR = "csdm";
    public static final String DOI_SELECTOR = "doi";
    public static final String DEFAULT_SELECTOR = "default";
    private static final String DOWNLOAD_FILE_LOCATION = "[url]";
    private static final String OFFICIAL_FILE_LOCATION = "[officialDoiUrl]";
    private static final String START_DATE_TIME = "[date]";
    private static final String QUERY_TITLE = "[queryTitle]";
    private static final String SEARCH_URL = "[searchUrl]";
    private static final String DOI_FAILURE_MESSAGE = "[doiFailureMessage]";
    private static final String BCCVL_IMPORT_ID = "[bccvlImportID]";
    private static final String SUPPORT = "[support]";
    private static final String UNIQUE_ID = "[uniqueId]";
    private static final String MY_DOWNLOADS_URL = "[myDownloadsUrl]";
    private static final String HUB_NAME = "[hubName]";

    protected static final Logger logger = Logger.getLogger(DownloadService.class);

    @Inject
    protected PersistentQueueDAO persistentQueueDAO;
    @Inject
    protected SearchDAO searchDAO;
    @Inject
    protected IndexDAO indexDao;
    @Inject
    protected RestOperations restTemplate;
    @Inject
    protected com.fasterxml.jackson.databind.ObjectMapper objectMapper;
    @Inject
    protected EmailService emailService;
    @Inject
    protected LoggerService loggerService;
    @Inject
    protected AbstractMessageSource messageSource;

    @Inject
    protected DoiService doiService;
    @Inject
    protected DataQualityService dataQualityService;

    // default value is supplied for the property below
    @Value("${webservices.root:http://localhost:8080/biocache-service}")
    public String webservicesRoot = "http://localhost:8080/biocache-service";

    // NC 20131018: Allow citations to be disabled via config (enabled by
    // default)
    @Value("${citations.enabled:true}")
    public Boolean citationsEnabled = Boolean.TRUE;

    // Allow headings information to be disabled via config (enabled by default)
    @Value("${headings.enabled:true}")
    public Boolean headingsEnabled = Boolean.TRUE;

    @Value("${download.readme.enabled:true}")
    public Boolean readmeEnabled = Boolean.TRUE;

    // Allow emailing support to be disabled via config (enabled by default)
    @Value("${download.support.email.enabled:true}")
    public Boolean supportEmailEnabled = Boolean.TRUE;

    @Value("${download.support.email:support@ala.org.au}")
    public String supportEmail = "support@ala.org.au";

    @Value("${data.description.url:headings.csv}")
    protected String dataFieldDescriptionURL = "headings.csv";

    @Value("${registry.url:https://collections.ala.org.au/ws}")
    protected String registryUrl = "https://collections.ala.org.au/ws";

    @Value("${citations.url:https://collections.ala.org.au/ws/citations}")
    protected String citationServiceUrl = "https://collections.ala.org.au/ws/citations";

    @Value("${download.email.subject:ALA Occurrence Download Complete - [filename]}")
    protected String biocacheDownloadEmailSubject = "ALA Occurrence Download Complete - [filename]";

    @Value("${download.email.template:}")
    protected String biocacheDownloadEmailTemplate;

    @Value("${download.doi.resolver:https://doi.ala.org.au/doi/}")
    public String alaDoiResolver;

    @Value("${my.download.doi.baseUrl:https://doi.ala.org.au/myDownloads}")
    public String myDownloadsUrl;

    @Value("${download.support:support@ala.org.au}")
    public String support;

    @Value("${download.doi.email.template:}")
    protected String biocacheDownloadDoiEmailTemplate;

    @Value("${download.email.subject.failure:Occurrence Download Failed - [filename]}")
    protected String biocacheDownloadEmailSubjectError = "Occurrence Download Failed - [filename]";

    @Value("${download.email.body.error:Your [hubName] download has failed.}")
    protected String biocacheDownloadEmailBodyError = "Your [hubName] download has failed.";

    @Value("${download.readme.template:}")
    protected String biocacheDownloadReadmeTemplate;

    @Value("${download.doi.readme.template:}")
    protected String biocacheDownloadDoiReadmeTemplate;

    @Value("${download.doi.failure.message:}")
    protected String biocacheDownloadDoiFailureMessage;

    @Value("${download.doi.title.prefix:Occurrence download }")
    protected String biocacheDownloadDoiTitlePrefix = "Occurrence download ";

    @Value("${download.doi.landing.page.baseUrl:https://doi-test.ala.org.au/doi/}")
    protected String biocacheDownloadDoiLandingPage = "https://doi-test.ala.org.au/doi/";

    @Value("${download.additional.local.files:}")
    protected String biocacheDownloadAdditionalLocalFiles;

    /**
     * A delay (in milliseconds) between minting the DOI, and sending emails containing
     * the DOI to allow for the DOI registration to propagate to upstream DOI providers.
     * <p>
     * Users have commented that the DOI is not resolvable when they receive the email
     * and this is the configuration setting to tweak to improve that behaviour.
     * <p>
     * Note that this delay starts after the chosen DOI provider has confirmed that
     * they have successfully minted the DOI. Hence, there are no issues with setting
     * it to zero if the DOI provider is known not to be propagating the registration
     * to another upstream DOI provider before it is resolvable.
     */
    @Value("${download.doi.propagation.delay:60000}")
    protected long doiPropagationDelay;

    /** Max number of threads to use in parallel for large offline download queries */
    @Value("${download.offline.parallelquery.maxthreads:30}")
    protected Integer maxOfflineParallelQueryDownloadThreads = 30;

    @Value("${download.offline.queue.maxsize:50}")
    protected Integer maxOfflineQueueMaxSize = 50;

    /** restrict the size of files in a zip */
    @Value("${zip.file.size.mb.max:4000}")
    public Integer maxMB;

    @Value("${download.url:https://biocache.ala.org.au/biocache-download}")
    public String biocacheDownloadUrl;

    @Value("${download.dir:/data/biocache-download}")
    public String biocacheDownloadDir;

    /**
     * Set to true to enable downloading of sensitive data
     */
    @Value("${download.auth.sensitive:false}")
    private Boolean downloadAuthSensitive;

    @Value("${biocache.ui.url:https://biocache.ala.org.au}")
    protected String biocacheUiUrl = "https://biocache.ala.org.au";

    //TODO: this should be retrieved from SDS
    @Value("${sensitiveAccessRoles20:{\n" +
            "\n" +
            "\"ROLE_SDS_ACT\" : \"sensitive:\\\"generalised\\\" AND (cl927:\\\"Australian Captial Territory\\\" OR cl927:\\\"Jervis Bay Territory\\\") AND -(dataResourceUid:dr359 OR dataResourceUid:dr571 OR dataResourceUid:dr570)\"\n" +
            "\"ROLE_SDS_NSW\" : \"sensitive:\\\"generalised\\\" AND cl927:\\\"New South Wales (including Coastal Waters)\\\" AND -(dataResourceUid:dr359 OR dataResourceUid:dr571 OR dataResourceUid:dr570)\",\n" +
            "\"ROLE_SDS_NZ\" : \"sensitive:\\\"generalised\\\" AND (dataResourceUid:dr2707 OR dataResourceUid:dr812 OR dataResourceUid:dr814 OR dataResourceUid:dr808 OR dataResourceUid:dr806 OR dataResourceUid:dr815 OR dataResourceUid:dr802 OR dataResourceUid:dr805 OR dataResourceUid:dr813) AND -cl927:* AND -(dataResourceUid:dr359 OR dataResourceUid:dr571 OR dataResourceUid:dr570)\",\n" +
            "\"ROLE_SDS_NT\" : \"sensitive:\\\"generalised\\\" AND cl927:\\\"Northern Territory (including Coastal Waters)\\\" AND -(dataResourceUid:dr359 OR dataResourceUid:dr571 OR dataResourceUid:dr570)\",\n" +
            "\"ROLE_SDS_QLD\" : \"sensitive:\\\"generalised\\\" AND cl927:\\\"Queensland (including Coastal Waters)\\\" AND -(dataResourceUid:dr359 OR dataResourceUid:dr571 OR dataResourceUid:dr570)\",\n" +
            "\"ROLE_SDS_SA\" : \"sensitive:\\\"generalised\\\" AND cl927:\\\"South Australia (including Coastal Waters)\\\" AND -(dataResourceUid:dr359 OR dataResourceUid:dr571 OR dataResourceUid:dr570)\",\n" +
            "\"ROLE_SDS_TAS\" : \"sensitive:\\\"generalised\\\" AND cl927:\\\"Tasmania (including Coastal Waters)\\\" AND -(dataResourceUid:dr359 OR dataResourceUid:dr571 OR dataResourceUid:dr570)\",\n" +
            "\"ROLE_SDS_VIC\" : \"sensitive:\\\"generalised\\\" AND cl927:\\\"Victoria (including Coastal Waters)\\\" AND -(dataResourceUid:dr359 OR dataResourceUid:dr571 OR dataResourceUid:dr570)\",\n" +
            "\"ROLE_SDS_WA\" : \"sensitive:\\\"generalised\\\" AND cl927:\\\"Western Australia (including Coastal Waters)\\\" AND -(dataResourceUid:dr359 OR dataResourceUid:dr571 OR dataResourceUid:dr570)\",\n" +
            "\"ROLE_SDS_BIRDLIFE\" : \"sensitive:\\\"generalised\\\" AND (dataResourceUid:dr359 OR dataResourceUid:dr571 OR dataResourceUid:dr570)\"\n" +
            "\n" +
            "}}")
    protected String sensitiveAccessRoles20 = "{}";

    private JSONObject sensitiveAccessRolesToSolrFilters20;

    @Value("${download.offline.max.url:https://downloads.ala.org.au}")
    public String dowloadOfflineMaxUrl = "https://downloads.ala.org.au";

    /**
     * By default this is set to a very large value to 'disable' the offline download limit.
     */
    @Value("${download.offline.max.size:100000000}")
    public Integer dowloadOfflineMaxSize = 100000000;

    @Value("${download.offline.msg:Too many records requested. Bulk download files for Lifeforms are available.}")
    public String downloadOfflineMsg = "Too many records requested. Bulk download files for Lifeforms are available.";

    @Value("${download.offline.msg:This download is unavailable. Run the download again.}")
    public String downloadOfflineMsgDeleted = "This download is unavailable. Run the download again.";

    @Value("${download.qualityFiltersTemplate:classpath:download-email-quality-filter-snippet.html}")
    public Resource downloadQualityFiltersTemplate;

    @Value("${download.date.format:dd MMMMM yyyy}")
    public String downloadDateFormat = "dd MMMMM yyyy";

    @Value("${download.csdm.email.template:}")
    protected String biocacheDownloadCSDMEmailTemplate;

    ConcurrentHashMap<String, ThreadPoolExecutor> userExecutors;

    @PostConstruct
    public void init() throws ParseException {
        // Simple JSON initialisation, let's follow the default Spring semantics
        sensitiveAccessRolesToSolrFilters20 = (JSONObject) new JSONParser().parse(sensitiveAccessRoles20);

        // Return download requests that were unfinished at shutdown
        Queue<DownloadDetailsDTO> fromPersistent = persistentQueueDAO.refreshFromPersistent();
        for (DownloadDetailsDTO dd : fromPersistent) {
            try {
                add(dd);
            } catch (TooManyDownloadRequestsException e) {
                // ignore
            } catch (IOException e) {
                logger.error("failed to add unfinished download to download queue, id: " + dd.getUniqueId() + ", " + e.getMessage());
            }
        }

        userExecutors = new ConcurrentHashMap<String, ThreadPoolExecutor>();
    }

    /**
     * Ensures that all of the download threads are given a chance to shutdown cleanly using thread interrupts when a Spring {@link ContextClosedEvent} occurs.
     */
    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        for(ThreadPoolExecutor ex : userExecutors.values()) {
            ex.shutdown();
        }
    }

    @Scheduled(fixedDelay = 43200000)// schedule to run every 12 hours
    public void removeUnusedExecutors() {
        for (Map.Entry<String, ThreadPoolExecutor> set : userExecutors.entrySet()) {
            if (set.getValue().getActiveCount() == 0) {
                userExecutors.remove(set.getKey());
                set.getValue().shutdown();
            }
        }
    }

    private boolean isAuthorisedSystem(DownloadDetailsDTO dd) {
        // TODO: this is required when the deprecated /occurrence/download is removed. Use JWT scope to test for permission.
        return false;
    }

    public void add(DownloadDetailsDTO dd) throws TooManyDownloadRequestsException, IOException {
        // default to one download per user
        int maxPoolSize = 1;

        if (isAuthorisedSystem(dd)) {
            // authorised systems are permitted a larger number of concurrent requests
            maxPoolSize = maxOfflineParallelQueryDownloadThreads;
        }

        // get the executor for this user
        ThreadPoolExecutor executor = userExecutors.get(getUserId(dd));

        // create a new executor when it does not exist
        if (executor == null) {
            LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();

            executor = new ThreadPoolExecutor(1, maxPoolSize, 0L, TimeUnit.MILLISECONDS, queue);
            userExecutors.put(getUserId(dd), executor);
        }

        if (executor.getQueue().size() >= maxOfflineQueueMaxSize) {
            throw new TooManyDownloadRequestsException();
        } else {
            persistentQueueDAO.add(dd);
            executor.execute(getDownloadRunnable(dd));
        }
    }

    private String getUserId(DownloadDetailsDTO dd) {
        String userId = "";
        if (dd.getAlaUser() != null && dd.getAlaUser().getUserId() != null) {
            userId = dd.getAlaUser().getUserId();
        }
        return userId;
    }

    protected DownloadRunnable getDownloadRunnable(DownloadDetailsDTO dd) {
        return new DownloadRunnable(dd);
    }

    /**
     * Returns a list of current downloads
     *
     * @return
     */
    public List<DownloadDetailsDTO> getCurrentDownloads() {
        List<DownloadDetailsDTO> result = new ArrayList<>();
        for (ThreadPoolExecutor ex : userExecutors.values()) {
            for (Runnable r : ex.getQueue()) {
                result.add(((DownloadRunnable) r).currentDownload);
            }
        }

        return Collections.unmodifiableList(result);
    }

    /**
     * Asynchronous
     *
     * Writes the supplied download to the supplied output stream. It will
     * include all the appropriate citations etc.
     *
     * @param dd
     * @param out
     * @param doiResponseList Return the CreateDoiResponse instance as the first element of the list if requestParams.mintDoi was true
     * @throws Exception
     */
    public void writeQueryToStream(DownloadDetailsDTO dd,
                                   OutputStream out,
                                   boolean limit,
                                   boolean zip,
                                   ExecutorService parallelExecutor,
                                   List<CreateDoiResponse> doiResponseList)
            throws Exception {
        DownloadRequestDTO requestParams = dd.getRequestParams();
        String filename = dd.getRequestParams().getFile();
        String originalParams = dd.getRequestParams().toString();

        String assertions = OccurrenceIndex.ASSERTIONS;
        String data_resource_uid = OccurrenceIndex.DATA_RESOURCE_UID;

        // Use a zip output stream to include the data and citation together in
        // the download.
        try (OptionalZipOutputStream sp = new OptionalZipOutputStream(
                zip ? OptionalZipOutputStream.Type.zipped : OptionalZipOutputStream.Type.unzipped, new CloseShieldOutputStream(out), maxMB);) {
            String suffix = requestParams.getFileType();
            sp.putNextEntry(filename + "." + suffix);
            // put the facets
            if ("all".equals(requestParams.getQa())) {
                requestParams.setFacets(new String[]{assertions, data_resource_uid});
            } else {
                requestParams.setFacets(new String[]{data_resource_uid});
            }

            final DownloadStats downloadStats = new DownloadStats();
            DownloadHeaders downloadHeaders = searchDAO.writeResultsFromIndexToStream(
                    requestParams, sp, downloadStats, dd, limit, parallelExecutor);

            sp.closeEntry();

            // continue if the download is not cancelled
            if (!dd.getInterrupt()) {
                // Add the data citation to the download
                List<String> citationsForReadme = new ArrayList<String>();

                Boolean mintDoi = requestParams.getMintDoi();
                CreateDoiResponse doiResponse = null;
                String doi = "";

                Map<String, String> enabledQualityFiltersByLabel = dataQualityService.getEnabledFiltersByLabel(requestParams);
                List<QualityFilterDTO> qualityFilters = getQualityFilterDTOS(enabledQualityFiltersByLabel);
                final String searchUrl = generateSearchUrl(requestParams, enabledQualityFiltersByLabel);
                String dqFixedSearchUrl = dataQualityService.convertDataQualityParameters(searchUrl, enabledQualityFiltersByLabel);

                if (citationsEnabled) {
                    List<Map<String, String>> datasetMetadata = null;
                    if (mintDoi) {
                        datasetMetadata = new ArrayList<>();
                    }

                    // add the citations for the supplied uids
                    sp.putNextEntry("citation.csv");
                    try {
                        getCitations(downloadStats.getUidStats(), sp, requestParams.getSep(), requestParams.getEsc(), citationsForReadme, datasetMetadata);
                    } catch (IOException e) {
                        logger.error(e.getMessage(), e);
                    }
                    sp.closeEntry();

                    if (mintDoi) {

                        // Prepare licence
                        Set<String> datasetLicences = downloadStats.getLicences();
                        List<String> licence = Lists.newArrayList(datasetLicences);

                        try {
                            DownloadDoiDTO doiDetails = new DownloadDoiDTO();

                            doiDetails.setTitle(biocacheDownloadDoiTitlePrefix + filename);
                            doiDetails.setApplicationUrl(dqFixedSearchUrl);
                            doiDetails.setRequesterId(dd.getAlaUser() == null ? null : getUserId(dd));
                            doiDetails.setRequesterName(dd.getAlaUser() == null ? null : dd.getAlaUser().getGivenName() + " " + dd.getAlaUser().getFamilyName());
                            doiDetails.setAuthorisedRoles(dd.getAlaUser() == null ? Collections.emptySet() : dd.getAlaUser().getRoles());
                            doiDetails.setDatasetMetadata(datasetMetadata);
                            doiDetails.setRequestTime(dd.getStartDateString());
                            doiDetails.setRecordCount(dd.getTotalRecords());
                            doiDetails.setLicence(licence);
                            doiDetails.setQueryTitle(requestParams.getDisplayString());
                            doiDetails.setApplicationMetadata(requestParams.getDoiMetadata());
                            if (StringUtils.isNotBlank(requestParams.getQualityProfile())) {
                                doiDetails.setDataProfile(dataQualityService.getProfileFullName(requestParams.getQualityProfile()));
                            }
                            doiDetails.setQualityFilters(qualityFilters);
                            doiDetails.setDisplayTemplate(requestParams.getDoiDisplayTemplate());

                            doiResponse = doiService.mintDoi(doiDetails);

                        } catch (Exception e) {
                            logger.error("DOI minting failed", e);
                        }
                        if (doiResponse != null) {
                            logger.debug("DOI minted: " + doiResponse.getDoi());
                            doiResponseList.add(doiResponse);
                        } else {
                            logger.error("DOI minting failed for path " + dd.getFileLocation());
                        }
                    }
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Not adding citation. Enabled: " + citationsEnabled + " uids: " + downloadStats.getUidStats());
                    }
                }

                // online downloads will not have a file location or request params set
                // in dd.
                if (dd.getRequestParams() == null) {
                    dd.setRequestParams(requestParams);
                }
                if (dd.getFileLocation() == null) {
                    dd.setFileLocation(dqFixedSearchUrl);
                }

                if (readmeEnabled) {
                    // add the Readme for the data field descriptions
                    sp.putNextEntry("README.html");
                    String dataProviders = "<ul><li>" + StringUtils.join(citationsForReadme, "</li><li>") + "</li></ul>";

                    String readmeFile;
                    String fileLocation;

                    if (mintDoi && doiResponse != null) {
                        readmeFile = biocacheDownloadDoiReadmeTemplate;
                        doi = doiResponse.getDoi();
                        // TODO: The downloads-plugin has issues with unencoded user queries
                        // Working around that by hardcoding the official DOI resolution service as the landing page
                        // https://github.com/AtlasOfLivingAustralia/biocache-service/issues/311
                        fileLocation = OFFICIAL_DOI_RESOLVER + doi;

                    } else {
                        readmeFile = biocacheDownloadReadmeTemplate;
                        fileLocation = dd.getFileLocation().replace(biocacheDownloadDir, biocacheDownloadUrl);
                    }

                    String readmeTemplate = "";
                    if (readmeFile != null && new File(readmeFile).exists()) {
                        readmeTemplate = FileUtils.readFileToString(new File(readmeFile), StandardCharsets.UTF_8);
                    }

                    String dataQualityFilters = "";
                    if (!qualityFilters.isEmpty()) {
                        dataQualityFilters = getDataQualityFiltersString(qualityFilters);
                    }

                    String readmeContent = readmeTemplate.replace("[url]", fileLocation)
                            .replace("[date]", dd.getStartDateString(downloadDateFormat))
                            .replace("[searchUrl]", dqFixedSearchUrl)
                            .replace("[queryTitle]", dd.getRequestParams().getDisplayString())
                            .replace("[dataProviders]", dataProviders)
                            .replace("[dataQualityFilters]", dataQualityFilters);

                    sp.write(readmeContent.getBytes(StandardCharsets.UTF_8));
                    sp.write(("For more information about the fields that are being downloaded please consult <a href='"
                            + dataFieldDescriptionURL + "'>Download Fields</a>.").getBytes(StandardCharsets.UTF_8));
                    sp.closeEntry();
                }

                if (mintDoi && doiResponse != null) {

                    sp.putNextEntry("doi.txt");

                    sp.write((OFFICIAL_DOI_RESOLVER + doiResponse.getDoi()).getBytes(StandardCharsets.UTF_8));
                    sp.write(CSVWriter.DEFAULT_LINE_END.getBytes(StandardCharsets.UTF_8));
                    sp.closeEntry();
                }

                // Add headings file, listing information about the headings
                if (headingsEnabled) {
                    // add the citations for the supplied uids
                    sp.putNextEntry("headings.csv");
                    try {
                        getHeadings(downloadHeaders, sp, requestParams, dd.getMiscFields());
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                    sp.closeEntry();
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Not adding header. Enabled: " + headingsEnabled + " uids: " + downloadStats.getUidStats());
                    }
                }

                if ((biocacheDownloadAdditionalLocalFiles != null) && !biocacheDownloadAdditionalLocalFiles.isEmpty()) {
                    String[] localFiles = biocacheDownloadAdditionalLocalFiles.split(",");
                    for (String localFile : localFiles) {
                        File f = new File(localFile);
                        if (f.exists()) {
                            sp.putNextEntry(f.getName());
                            sp.write(IOUtils.toByteArray(new FileInputStream(f)));
                            sp.closeEntry();
                        }
                    }
                }

                sp.flush();

                // now construct the sourceUrl for the log event
                String sourceUrl = originalParams.contains("qid:") ? webservicesRoot + "?" + requestParams.toString()
                        : webservicesRoot + "?" + originalParams;

                // log the stats to ala logger
                LogEventVO vo = new LogEventVO(1002, requestParams.getReasonTypeId(), requestParams.getSourceTypeId(),
                        requestParams.getEmail(), requestParams.getReason(), dd.getIpAddress(), dd.getUserAgent(), null, downloadStats.getUidStats(), sourceUrl);

                loggerService.logEvent(vo);
            }
        } catch (RecordWriterException e) {
            logger.error(e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // sApplication may be shutting down, do not delete the download file
            throw e;
        }
    }

    private List<QualityFilterDTO> getQualityFilterDTOS(Map<String, String> filtersByLabel) {
        return filtersByLabel.entrySet().stream().map((e) -> new QualityFilterDTO(e.getKey(), e.getValue())).collect(toList());
    }

    @VisibleForTesting
    String getDataQualityFiltersString(List<QualityFilterDTO> qualityFilters) throws IOException, org.apache.velocity.runtime.parser.ParseException {
        String dataQualityFilters;
        RuntimeServices runtimeServices = RuntimeSingleton.getRuntimeServices();
        runtimeServices.setProperty(RuntimeConstants.RUNTIME_LOG_LOGSYSTEM_CLASS, org.apache.velocity.runtime.log.Log4JLogChute.class.getName());
        runtimeServices.setProperty("runtime.log.logsystem.log4j.logger", "velocity");
        Reader reader = new InputStreamReader(downloadQualityFiltersTemplate.getInputStream(), StandardCharsets.UTF_8);
        Template template = new Template();
        template.setRuntimeServices(runtimeServices);

        template.setData(runtimeServices.parse(reader, "download-quality-filters-template"));

        template.initDocument();
        StringWriter sw = new StringWriter();

        VelocityContext context = new VelocityContext();

        context.put("qualityFilters", qualityFilters);

        template.merge(context, sw);
        dataQualityFilters = sw.toString();
        return dataQualityFilters;
    }

    /**
     * Synchronous
     * @param requestParams
     * @param response
     * @param alaUser
     * @param ip
     * @param userAgent
     * @param out
     * @param zip
     * @param parallelQueryExecutor
     * @throws Exception
     */
    public void writeQueryToStream(DownloadRequestDTO requestParams,
                                   HttpServletResponse response,
                                   AlaUserProfile alaUser,
                                   String ip,
                                   String userAgent,
                                   OutputStream out,
                                   boolean zip,
                                   ExecutorService parallelQueryExecutor) throws Exception {
        String filename = requestParams.getFile();

        response.setHeader("Cache-Control", "must-revalidate");
        response.setHeader("Pragma", "must-revalidate");

        if (zip) {
            response.setHeader("Content-Disposition", "attachment;filename=" + filename + ".zip");
            response.setContentType("application/zip");
        } else {
            response.setHeader("Content-Disposition", "attachment;filename=" + filename + ".txt");
            response.setContentType("text/plain");
        }

        DownloadDetailsDTO.DownloadType type = DownloadType.RECORDS_INDEX;
        DownloadDetailsDTO dd = new DownloadDetailsDTO(requestParams, alaUser, ip, userAgent, type);
        writeQueryToStream(dd, new CloseShieldOutputStream(out), true, zip, parallelQueryExecutor, null);
    }

    /**
     * get citation info from citation web service and write it into
     * citation.txt file.
     *
     * @param uidStats
     * @param out
     * @param datasetMetadata
     * @throws HttpException
     * @throws IOException
     */
    public void getCitations(ConcurrentMap<String, AtomicInteger> uidStats, OutputStream out, char sep, char esc,
                             List<String> readmeCitations, List<Map<String, String>> datasetMetadata) throws IOException {

        if (citationsEnabled) {

            if (uidStats == null) {
                logger.error("Unable to generate citations: logger statistics was null", new Exception().fillInStackTrace());
                return;
            } else if (out == null) {
                logger.error("Unable to generate citations: output stream was null", new Exception().fillInStackTrace());
                return;
            }

            try (CSVWriter writer = new CSVWriter(new OutputStreamWriter(new CloseShieldOutputStream(out), StandardCharsets.UTF_8), sep, '"', esc);) {
                // Always write something to the citations.csv file so that users can distinguish between cases themselves when reporting issues
                // i18n of the citation header
                writer.writeNext(new String[]{
                    messageSource.getMessage("citation.uid", null, "UID", null),
                    messageSource.getMessage("citation.name", null, "Name", null),
                    messageSource.getMessage("citation.doi", null, "DOI", null),
                    messageSource.getMessage("citation.citation", null, "Citation", null),
                    messageSource.getMessage("citation.rights", null, "Rights", null),
                    messageSource.getMessage("citation.link", null, "More Information", null),
                    messageSource.getMessage("citation.dataGeneralizations", null, "Data generalisations", null),
                    messageSource.getMessage("citation.informationWithheld", null, "Information withheld", null),
                    messageSource.getMessage("citation.downloadLimit", null, "Download limit", null),
                    messageSource.getMessage("citation.count", null, "Number of Records in Download", null)
                });

                if (!uidStats.isEmpty()) {
                    List<LinkedHashMap<String, Object>> entities = restTemplate.postForObject(citationServiceUrl,
                            uidStats.keySet(), List.class);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Posting to " + citationServiceUrl);
                        logger.debug("UIDs " + uidStats.keySet().stream().collect(Collectors.joining(",")));
                    }
                    final int UID = 0;
                    final int NAME = 1;
                    final int CITATION = 3;
                    final int RIGHTS = 4;
                    final int LINK = 5;
                    final int COUNT = 9;

                    List<Map<String, Object>> useableRecords = entities.stream()
                            .filter(m -> m != null && m.get("uid") != null).collect(toList());

                    for (Map<String, Object> record : useableRecords) {
                        Object uid = record.get("uid");
                        AtomicInteger uidRecordCount = uidStats.get(uid);
                        String count = Optional.ofNullable(uidRecordCount).orElseGet(() -> new AtomicInteger(0)).toString();
                        String[] row = new String[]{
                                (String) record.getOrDefault( "uid", ""),
                                (String) record.getOrDefault( "name", ""),
                                (String) record.getOrDefault( "DOI", ""),
                                (String) record.getOrDefault( "citation", ""),
                                (String) record.getOrDefault( "rights", ""),
                                (String) record.getOrDefault( "link", ""),
                                (String) record.getOrDefault( "dataGeneralizations", ""),
                                (String) record.getOrDefault( "informationWithheld", ""),
                                (String) record.getOrDefault( "downloadLimit", ""),
                                count};
                        writer.writeNext(row);

                        if (readmeCitations != null) {
                            // used in README.txt
                            readmeCitations.add(row[CITATION] + " (" + row[RIGHTS] + "). " + row[LINK]);
                        }

                        if (datasetMetadata != null) {
                            Map<String, String> dataSet = new HashMap<>();

                            dataSet.put("uid", row[UID]);
                            dataSet.put("name", row[NAME]);
                            dataSet.put("licence", row[RIGHTS]);
                            dataSet.put("count", row[COUNT]);

                            datasetMetadata.add(dataSet);
                        }
                    }

                    if (useableRecords.size() < uidStats.keySet().size()) {
                        List<String> usedUids = useableRecords.stream().map(record -> (String)record.get("uid")).collect(toList());
                        String missingUids = uidStats.keySet().stream().filter(uid -> !usedUids.contains(uid)).collect(Collectors.joining());
                        logger.warn("The following UIDs will not have citations (missing in registry): " + missingUids);
                    }

                } else {
                    logger.warn("No collected stats for a download");
                }
                writer.flush();
            }
        }
    }

    /**
     * get headings info from index/fields web service and write it into
     * headings.csv file.
     * <p>
     * output columns: column name field requested dwc description info field
     *
     * @param out
     * @throws HttpException
     * @throws IOException
     */
    public void getHeadings(DownloadHeaders downloadHeaders, OutputStream out,
                            DownloadRequestDTO params, String[] miscHeaders) throws Exception {
        if (headingsEnabled) {

            if (out == null) {
                logger.error("Unable to generate headings info: output stream was null", new Exception().fillInStackTrace());
                return;
            }

            try (CSVWriter writer = new CSVWriter(new OutputStreamWriter(new CloseShieldOutputStream(out), StandardCharsets.UTF_8), params.getSep(), '"',
                    params.getEsc());) {
                Set<IndexFieldDTO> indexedFields = indexDao.getIndexedFields();

                // header
                writer.writeNext(new String[]{"Column name", "Requested field", "DwC Name", "Field name",
                        "Field description", "Download field name", "Download field description", "More information"});

                String[] fieldsRequested = downloadHeaders.joinOriginalIncluded();
                String[] headerOutput = downloadHeaders.joinedHeader();

                if (fieldsRequested != null && headerOutput != null) {
                    // ignore first fieldsRequested and headerOutput record
                    for (int i = 1; i < fieldsRequested.length && i < headerOutput.length; i++) {

                        // find indexedField by download name
                        IndexFieldDTO ifdto = null;
                        for (IndexFieldDTO f : indexedFields) {
                            // find a matching field
                            if (fieldsRequested[i].equalsIgnoreCase(f.getDownloadName())) {
                                ifdto = f;
                                break;
                            }
                        }
                        // find indexedField by field name
                        if (ifdto == null) {
                            for (IndexFieldDTO f : indexedFields) {
                                // find a matching field
                                if (fieldsRequested[i].equalsIgnoreCase(f.getName())) {
                                    ifdto = f;
                                    break;
                                }
                            }
                        }

                        if (ifdto != null && StringUtils.isNotEmpty(headerOutput[i])) {
                            writer.writeNext(new String[]{headerOutput[i], fieldsRequested[i],
                                    ifdto.getDwcTerm() != null ? ifdto.getDwcTerm() : "",
                                    ifdto.getName() != null ? ifdto.getName() : "",
                                    ifdto.getDescription() != null ? ifdto.getDescription() : "",
                                    ifdto.getDownloadName() != null ? ifdto.getDownloadName() : "",
                                    ifdto.getDownloadDescription() != null ? ifdto.getDownloadDescription() : "",
                                    ifdto.getInfo() != null ? ifdto.getInfo() : ""});
                        } else if (StringUtils.isNotEmpty(headerOutput[i])) {
                            // others, e.g. species lists and analysis layers that do not appear in the fieldsRequested
                            // but do appear in the header. Do not include the species list ids or analysis ids
                            String info = messageSource.getMessage("description." + fieldsRequested[i], null, "", null);
                            writer.writeNext(new String[]{headerOutput[i], fieldsRequested[i], "", "", "", "", "",
                                    info != null ? info : ""});
                        }
                    }
                }

                // misc headers
                if (miscHeaders != null) {
                    String defaultDescription = messageSource.getMessage("description.", null, "Raw field from data provider.", null);
                    for (int i = 0; i < miscHeaders.length; i++) {
                        writer.writeNext(
                                new String[]{miscHeaders[i], "", "", "", "", "", messageSource.getMessage("description." + miscHeaders[i], null, defaultDescription, null)});
                    }
                }

                writer.flush();
            }
        }
    }

    /**
     * Generate a search URL the user can use to regenerate the same download
     * (assumes they came via biocache UI)
     *
     * @param params
     * @return url
     */
    public String generateSearchUrl(DownloadRequestDTO params) {
        return generateSearchUrl(params, null);
    }

    /**
     * Generate a search URL the user can use to regenerate the same download
     * (assumes they came via biocache UI) using pre-supplied quality filters
     *
     * @param params                       The download / search parameters to use
     * @param enabledQualityFiltersByLabel A pre-provided map of enabled quality filter label to fqs or null if the should be looked up on demand.
     * @return url The generated search url
     */
    public String generateSearchUrl(DownloadRequestDTO params, @Nullable Map<String, String> enabledQualityFiltersByLabel) {
        if (params.getSearchUrl() != null) {
            return params.getSearchUrl();
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append(biocacheUiUrl + "/occurrences/search?");

            if (params.getQId() != null) {
                try {
                    sb.append("qid=").append(URLEncoder.encode("" + params.getQId(), "UTF-8"));
                } catch (UnsupportedEncodingException e) {

                }
            }
            if (params.getQ() != null) {
                try {
                    sb.append("&q=").append(URLEncoder.encode(params.getQ(), "UTF-8"));
                } catch (UnsupportedEncodingException e) {

                }
            }

            if (params.getFq().length > 0) {
                for (String fq : params.getFq()) {
                    if (StringUtils.isNotEmpty(fq)) {
                        try {
                            sb.append("&fq=").append(URLEncoder.encode(fq, "UTF-8"));
                        } catch (UnsupportedEncodingException e) {

                        }
                    }
                }
            }

            if (params.isDisableAllQualityFilters()) {
                sb.append("&disableAllQualityFilters=true");
            } else {
                sb.append("&disableAllQualityFilters=true");

                if (enabledQualityFiltersByLabel == null) {
                    enabledQualityFiltersByLabel = dataQualityService.getEnabledFiltersByLabel(params);
                }
                enabledQualityFiltersByLabel.forEach((label, fq) -> {
                    try {
                        sb.append("&fq=").append(URLEncoder.encode(fq, "UTF-8"));
                    } catch (UnsupportedEncodingException ignored) {
                    }
                });
            }

            if (StringUtils.isNotEmpty(params.getQc())) {
                try {
                    sb.append("&qc=").append(URLEncoder.encode(params.getQc(), "UTF-8"));
                } catch (UnsupportedEncodingException e) {

                }
            }

            if (StringUtils.isNotEmpty(params.getWkt())) {
                try {
                    sb.append("&wkt=").append(URLEncoder.encode(params.getWkt(), "UTF-8"));
                } catch (UnsupportedEncodingException e) {

                }
            }

            if (params.getLat() != null && params.getLon() != null && params.getRadius() != null) {
                sb.append("&lat=").append(params.getLat());
                sb.append("&lon=").append(params.getLon());
                sb.append("&radius=").append(params.getRadius());
            }

            return sb.toString();
        }
    }

    private void insertMiscHeader(DownloadDetailsDTO download) {
        if (download.getMiscFields() != null && download.getMiscFields().length > 0
                && download.getRequestParams() != null) {
            try {
                // unpack zip
                File unzipDir = new File(download.getFileLocation() + ".dir" + File.separator);
                unzipDir.mkdirs();
                AlaFileUtils.unzip(unzipDir.getPath(), download.getFileLocation());

                // insert header
                for (File f : unzipDir.listFiles()) {
                    if ((f.getName().endsWith(".csv") || f.getName().endsWith(".tsv"))
                            && !"headings.csv".equals(f.getName())) {
                        // make new file
                        try (FileReader fileReader = new FileReader(f);
                             BufferedReader bufferedReader = new BufferedReader(fileReader);) {
                            File fnew = new File(f.getPath() + ".new");
                            try (FileWriter fw = new FileWriter(fnew);) {
                                String line;
                                int row = 0;
                                while ((line = bufferedReader.readLine()) != null) {
                                    if (row == 0) {
                                        String miscHeader[] = download.getMiscFields();

                                        if ("csv".equals(download.getRequestParams().getFileType())) {
                                            // retain csv settings
                                            CSVReader reader = new CSVReader(new StringReader(line));
                                            String header[] = reader.readNext();
                                            reader.close();

                                            String newHeader[] = new String[header.length + miscHeader.length];
                                            if (header.length > 0)
                                                System.arraycopy(header, 0, newHeader, 0, header.length);
                                            if (miscHeader.length > 0)
                                                System.arraycopy(miscHeader, 0, newHeader, header.length,
                                                        miscHeader.length);

                                            StringWriter sw = new StringWriter();
                                            try (CSVWriter writer = new CSVWriter(sw,
                                                    download.getRequestParams().getSep(), '"',
                                                    download.getRequestParams().getEsc());) {
                                                writer.writeNext(newHeader);
                                            }

                                            // remove the newline character at the end of this line
                                            line = sw.toString().trim();
                                        } else {
                                            for (int i = 0; i < miscHeader.length; i++) {
                                                line += '\t';
                                                line += miscHeader[i].replace("\r", "").replace("\n", "").replace("\t",
                                                        "");
                                            }
                                            line += '\n';
                                        }
                                    } else {
                                        fw.write("\n");
                                    }
                                    fw.write(line);
                                    row++;
                                }
                            }
                            // replace original file
                            FileUtils.copyFile(fnew, f);
                            fnew.delete();
                        }
                    }
                }

                // rezip and cleanup
                FileUtils.deleteQuietly(new File(download.getFileLocation()));
                AlaFileUtils.createZip(unzipDir.getPath(), download.getFileLocation());
                FileUtils.deleteDirectory(unzipDir);
            } catch (Exception e) {
                logger.error("failed to append misc header", e);
            }
        }
    }

    /**
     * Generates the Solr filter to query sensitive data for the user sensitive roles
     *
     * @return A String with a Solr filter
     */
    public String getSensitiveFq(Set<String> userRoles) {

        if (downloadAuthSensitive == null || !downloadAuthSensitive) {
            return null;
        }

        List<String> sensitiveRoles = new ArrayList<>(sensitiveAccessRolesToSolrFilters20.keySet());
        sensitiveRoles.retainAll(userRoles);

        String sensitiveFq = "";

        for (String sensitiveRole : sensitiveRoles) {
            if (sensitiveFq.length() > 0) {
                sensitiveFq += " OR ";
            }
            sensitiveFq += "(" + sensitiveAccessRolesToSolrFilters20.get(sensitiveRole) + ")";
        }

        if (sensitiveFq.length() == 0) {
            return null;
        }

        return sensitiveFq;
    }

    public String getEmailTemplateFile(DownloadDetailsDTO currentDownload) {
        String file;
        switch (currentDownload.getRequestParams().getEmailTemplate()) {
            case CSDM_SELECTOR:
                file = biocacheDownloadCSDMEmailTemplate;
                break;
            case DOI_SELECTOR:
                file = biocacheDownloadDoiEmailTemplate;
                break;
            case DEFAULT_SELECTOR:
            default:
                file = biocacheDownloadEmailTemplate;
                break;
        }

        return file;
    }

    public String getFailEmailBodyTemplate(DownloadDetailsDTO currentDownload) {
        String emailTemplate;
        switch (currentDownload.getRequestParams().getEmailTemplate()) {
            case CSDM_SELECTOR:
                emailTemplate = messageSource.getMessage("offlineFailEmailBodyCSDM", null, "", null);
                break;
            case DOI_SELECTOR:
            case DEFAULT_SELECTOR:
            default:
                emailTemplate = messageSource.getMessage("offlineFailEmailBody", null, "", null);
                break;
        }

        return emailTemplate;
    }

    public String generateEmailContent(String template, Map<String, String> substitutions) {
        if (template != null && substitutions.size() > 0) {
            for (Map.Entry<String, String> entry : substitutions.entrySet()) {
                template = template.replace(entry.getKey(), entry.getValue());
            }
        }

        return template;
    }

    public void cancel(DownloadDetailsDTO dd) throws InterruptedException {

        // remove from persistent queue (disk)
        persistentQueueDAO.remove(dd);

        // signal download to end
        dd.setInterrupt(true);

        // wait a short time for the download to end itself should it be running
        Thread.sleep(500);

        // get executor for this user
        ThreadPoolExecutor ex = userExecutors.get(getUserId(dd));
        if (ex != null) {
            // remove from executor queue
            for (Runnable r : ex.getQueue()) {
                if (((DownloadRunnable) r).currentDownload.getUniqueId().equals(dd.getUniqueId())) {
                    ex.remove(r);
                }
            }

            // finally, remove any output files
            File outputFile = new File(dd.getFileLocation());
            if (outputFile.exists()) {
                outputFile.delete();
            }

        }
    }

    public List<DownloadDetailsDTO> getDownloadsForUserId(String userId) {
        List<DownloadDetailsDTO> all = new ArrayList<>();
        for (DownloadDetailsDTO dd : persistentQueueDAO.getAllDownloads()) {
            if (userId.equalsIgnoreCase(getUserId(dd))) {
                all.add(dd);
            }
        }
        return all;
    }

    protected class DownloadRunnable implements Runnable {
        public DownloadDetailsDTO currentDownload;

        DownloadRunnable(DownloadDetailsDTO dd) {
            this.currentDownload = dd;
        }

        @Override
        public void run() {
            if (logger.isInfoEnabled()) {
                logger.info("Starting to download the offline request: " + currentDownload);
            }
            // we are now ready to start the download
            // we need to create an output stream to the file system

            boolean shuttingDown = false;
            boolean doRetry = false;

            try (FileOutputStream fos = FileUtils.openOutputStream(new File(currentDownload.getFileLocation()));) {
                List<CreateDoiResponse> doiResponseList = null;
                Boolean mintDoi = currentDownload.getRequestParams().getMintDoi();

                String doiFailureMessage = "";
                if (mintDoi) {
                    doiResponseList = new ArrayList<>();
                }

                logger.info("Writing download to file " + mintDoi);
                writeQueryToStream(
                        currentDownload,
                        new CloseShieldOutputStream(fos),
                        currentDownload.getDownloadType() == DownloadType.RECORDS_INDEX,
                        true,
                        null,
                        doiResponseList
                );

                // continue if not cancelled
                if (!currentDownload.getInterrupt()) {
                    logger.info("Sending email to recipient mintDoi " + mintDoi);
                    if (mintDoi && doiResponseList.size() <= 0) {
                        //DOI Minting failed
                        doiFailureMessage = biocacheDownloadDoiFailureMessage;
                        mintDoi = false; //Prevent any updates
                    }

                    logger.info("Sending email to recipient");
                    // now that the download is complete email a link to the
                    // recipient.
                    final String hubName = currentDownload.getRequestParams().getHubName() != null ? currentDownload.getRequestParams().getHubName() : "ALA";
                    String subject = messageSource.getMessage("offlineEmailSubject", null, biocacheDownloadEmailSubject, null)
                            .replace("[filename]", currentDownload.getRequestParams().getFile())
                            .replace("[hubName]", hubName);

                    logger.info("currentDownload = " + currentDownload);

                    if (currentDownload != null && currentDownload.getFileLocation() != null) {

                        logger.info("currentDownload.getFileLocation() = " + currentDownload.getFileLocation());
                        insertMiscHeader(currentDownload);

                        //ensure new directories and download file have correct permissions
                        new File(currentDownload.getFileLocation()).setReadable(true, false);
                        new File(currentDownload.getFileLocation()).getParentFile().setReadable(true, false);
                        new File(currentDownload.getFileLocation()).getParentFile().getParentFile().setReadable(true, false);
                        new File(currentDownload.getFileLocation()).getParentFile().setExecutable(true, false);
                        new File(currentDownload.getFileLocation()).getParentFile().getParentFile().setExecutable(true, false);

                        String archiveFileLocation = biocacheDownloadUrl + File.separator + URLEncoder.encode(currentDownload.getFileLocation().replace(biocacheDownloadDir + "/", ""), "UTF-8").replace("%2F", "/").replace("+", "%20");
                        final String searchUrl = generateSearchUrl(currentDownload.getRequestParams());
                        String doiStr = "";
                        String emailTemplate;
                        String emailTemplateFile;
                        Map<String, String> substitutions = new HashMap<>();
                        substitutions.put(START_DATE_TIME, currentDownload.getStartDateString(downloadDateFormat));
                        substitutions.put(QUERY_TITLE, currentDownload.getRequestParams().getDisplayString());
                        substitutions.put(SEARCH_URL, searchUrl);
                        substitutions.put(DOI_FAILURE_MESSAGE, doiFailureMessage);

                        if (mintDoi && doiResponseList != null && !doiResponseList.isEmpty() && doiResponseList.get(0) != null) {

                            CreateDoiResponse doiResponse;
                            doiResponse = doiResponseList.get(0);
                            try {
                                doiService.updateFile(doiResponse.getUuid(), currentDownload.getFileLocation());
                                doiStr = doiResponse.getDoi();
                                if (currentDownload.getRequestParams().getEmailTemplate() == DEFAULT_SELECTOR) {
                                    currentDownload.getRequestParams().setEmailTemplate(DOI_SELECTOR);
                                }

                                // TODO: The downloads-plugin has issues with unencoded user queries
                                // Working around that by hardcoding the official DOI resolution service as the landing page
                                // https://github.com/AtlasOfLivingAustralia/biocache-service/issues/311
                                substitutions.put(DOWNLOAD_FILE_LOCATION, alaDoiResolver + doiStr);
                                substitutions.put(OFFICIAL_FILE_LOCATION, OFFICIAL_DOI_RESOLVER + doiStr);
                                substitutions.put(BCCVL_IMPORT_ID, URLEncoder.encode(doiStr, "UTF-8"));
                            } catch (Exception ex) {
                                logger.error("DOI update failed for DOI uuid " + doiResponse.getUuid() +
                                        " and path " + currentDownload.getFileLocation(), ex);
                                currentDownload.getRequestParams().setEmailTemplate(DEFAULT_SELECTOR);
                                substitutions.put(DOWNLOAD_FILE_LOCATION, archiveFileLocation);
                            }
                        } else {
                            currentDownload.getRequestParams().setEmailTemplate(DEFAULT_SELECTOR);
                            substitutions.put(DOWNLOAD_FILE_LOCATION, archiveFileLocation);
                        }

                        logger.info("currentDownload.getRequestParams().isEmailNotify()  = " + currentDownload.getRequestParams().isEmailNotify());

                        if (currentDownload.getRequestParams().isEmailNotify()) {

                            // save the statistics to the download directory
                            try (FileOutputStream statsStream = FileUtils
                                    .openOutputStream(new File(new File(currentDownload.getFileLocation()).getParent()
                                            + File.separator + "downloadStats.json"))) {
                                objectMapper.writeValue(statsStream, currentDownload);
                            }

                            emailTemplateFile = getEmailTemplateFile(currentDownload);
                            emailTemplate = FileUtils.readFileToString(new File(emailTemplateFile), StandardCharsets.UTF_8);
                            String emailBody = generateEmailContent(emailTemplate, substitutions);

                            // save the statistics to the download directory
                            try (FileOutputStream statsStream = FileUtils
                                    .openOutputStream(new File(new File(currentDownload.getFileLocation()).getParent()
                                            + File.separator + "downloadStats.json"))) {
                                objectMapper.writeValue(statsStream, currentDownload);
                            }

                            logger.info("Delay sending the email to allow.....");
                            if (mintDoi && doiResponseList != null && !doiResponseList.isEmpty() && doiResponseList.get(0) != null) {
                                // Delay sending the email to allow the DOI to propagate through to upstream DOI providers
                                logger.info("Delay sending the email to allow....." + doiPropagationDelay);
                                Thread.sleep(doiPropagationDelay);
                            }

                            logger.info("Sending email now to  " + currentDownload.getRequestParams().getEmail());
                            emailService.sendEmail(currentDownload.getRequestParams().getEmail(), subject, emailBody);
                            logger.info("Email sent to  " + currentDownload.getRequestParams().getEmail());
                        }
                    }
                }
                persistentQueueDAO.remove(currentDownload);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                //shutting down
                shuttingDown = true;
            } catch (Exception e) {
                logger.error("Error in offline download, sending email. download path: "
                        + currentDownload.getFileLocation(), e);

                try {
                    final String hubName = currentDownload.getRequestParams().getHubName() != null ? currentDownload.getRequestParams().getHubName() : "ALA";
                    String subject = messageSource.getMessage("offlineEmailSubjectError", null, biocacheDownloadEmailSubjectError, null)
                            .replace("[filename]", currentDownload.getRequestParams().getFile())
                            .replace("[hubName]", hubName);

                    String copyTo = supportEmailEnabled ? supportEmail : null;

                    Map<String, String> substitutions = new HashMap<>();
                    substitutions.put(SEARCH_URL, generateSearchUrl(currentDownload.getRequestParams()));
                    substitutions.put(SUPPORT, support);
                    substitutions.put(UNIQUE_ID, currentDownload.getUniqueId());
                    substitutions.put(MY_DOWNLOADS_URL, myDownloadsUrl);
                    substitutions.put(HUB_NAME, hubName);
                    substitutions.put(DOWNLOAD_FILE_LOCATION, currentDownload.getFileLocation().replace(biocacheDownloadDir,
                            biocacheDownloadUrl));

                    String emailTemplate = getFailEmailBodyTemplate(currentDownload);
                    String emailBody = generateEmailContent(emailTemplate, substitutions);
                    // email error to user and support (configurable)
                    emailService.sendEmail(currentDownload.getRequestParams().getEmail(), copyTo, subject, emailBody);

                } catch (Exception ex) {
                    logger.error("Error sending error message to download email. "
                            + currentDownload.getFileLocation(), ex);
                }

                // If we ever want to retry on failure, enable doRetry and disable queue.remove
                //doRetry = true
                persistentQueueDAO.remove(currentDownload);
            } finally {
                // in case of server up/down, only remove from queue
                // after emails are sent
                if (currentDownload.getInterrupt() || (!shuttingDown && !doRetry)) {
                    persistentQueueDAO.remove(currentDownload);
                }
            }

            if (currentDownload.getInterrupt()) {
                // remove output file when cancelled
                new File(currentDownload.getFileLocation()).delete();
            }
        }
    }
}
