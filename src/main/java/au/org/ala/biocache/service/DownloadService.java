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
import au.org.ala.biocache.dao.PersistentQueueDAO;
import au.org.ala.biocache.dao.SearchDAO;
import au.org.ala.biocache.dto.DownloadDetailsDTO;
import au.org.ala.biocache.dto.DownloadDetailsDTO.DownloadType;
import au.org.ala.biocache.dto.DownloadDoiDTO;
import au.org.ala.biocache.dto.DownloadRequestParams;
import au.org.ala.biocache.dto.IndexFieldDTO;
import au.org.ala.biocache.dto.QualityFilterDTO;
import au.org.ala.biocache.stream.OptionalZipOutputStream;
import au.org.ala.biocache.util.AlaFileUtils;
import au.org.ala.biocache.util.thread.DownloadControlThread;
import au.org.ala.biocache.util.thread.DownloadCreator;
import au.org.ala.biocache.writer.RecordWriterException;
import au.org.ala.doi.CreateDoiResponse;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.ala.client.model.LogEventVO;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.runtime.RuntimeServices;
import org.apache.velocity.runtime.RuntimeSingleton;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.scale7.cassandra.pelops.exceptions.PelopsException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.support.AbstractMessageSource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestOperations;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.commons.lang3.StringUtils.*;

/**
 * Services to perform the downloads.
 * 
 * Can configure the number of off-line download processors
 *
 * @author Natasha Carter (natasha.carter@csiro.au)
 */
@Component("downloadService")
public class DownloadService implements ApplicationListener<ContextClosedEvent> {

    public static final String OFFICIAL_DOI_RESOLVER = "https://doi.org/";

    protected static final Logger logger = Logger.getLogger(DownloadService.class);
    /**
     * Download threads for matching subsets of offline downloads.
     * <br>
     * The default is:
     * <ul>
     * <li>4 threads for index (SOLR) downloads for &lt;50,000 occurrences with 10ms poll delay, 10ms execution delay, and normal thread priority (5)</li>
     * <li>1 thread for index (SOLR) downloads for &lt;100,000,000 occurrences with 100ms poll delay, 100ms execution delay, and minimum thread priority (1)</li>
     * <li>2 threads for db (CASSANDA) downloads for &lt;50,000 occurrences with 10ms poll delay, 10ms execution delay, and normal thread priority (5)</li>
     * <li>1 thread for either index or db downloads, an unrestricted count, with 300ms poll delay, 100ms execution delay, and minimum thread priority (1)</li>
     * </ul>
     *
     * If there are no thread patterns specified here, a single thread with 10ms poll delay and 0ms execution delay, and normal thread priority (5) will be created and used instead.
     */
    @Value("${concurrent.downloads.json:[{\"label\": \"smallSolr\", \"threads\": 4, \"maxRecords\": 50000, \"type\": \"index\", \"pollDelay\": 10, \"executionDelay\": 10, \"threadPriority\": 5}, {\"label\": \"largeSolr\", \"threads\": 1, \"maxRecords\": 100000000, \"type\": \"index\", \"pollDelay\": 100, \"executionDelay\": 100, \"threadPriority\": 1}, {\"label\": \"smallCassandra\", \"threads\": 1, \"maxRecords\": 50000, \"type\": \"db\", \"pollDelay\": 10, \"executionDelay\": 10, \"threadPriority\": 5}, {\"label\": \"defaultUnrestricted\", \"threads\": 1, \"pollDelay\": 1000, \"executionDelay\": 100, \"threadPriority\": 1}]}")
    protected String concurrentDownloadsJSON;
    @Inject
    protected PersistentQueueDAO persistentQueueDAO;
    @Inject
    protected SearchDAO searchDAO;
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
    protected AuthService authService;

    // when everything is indexed in SOLR, there will be no cassandra download unless requested
    @Value("${download.solr.only:false}")
    public Boolean downloadSolrOnly = Boolean.FALSE;

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

    /** Stores the current list of downloads that are being performed. */
    protected final Queue<DownloadDetailsDTO> currentDownloads = new LinkedBlockingQueue<DownloadDetailsDTO>();

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
     * 
     * Users have commented that the DOI is not resolvable when they receive the email 
     * and this is the configuration setting to tweak to improve that behaviour.
     * 
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
    @Value("${sensitiveAccessRoles:{\n" +
            "\n" +
            "\"ROLE_SDS_ACT\" : \"sensitive:\\\"generalised\\\" AND (cl927:\\\"Australian Captial Territory\\\" OR cl927:\\\"Jervis Bay Territory\\\") AND -(data_resource_uid:dr359 OR data_resource_uid:dr571 OR data_resource_uid:dr570)\"\n" +
            "\"ROLE_SDS_NSW\" : \"sensitive:\\\"generalised\\\" AND cl927:\\\"New South Wales (including Coastal Waters)\\\" AND -(data_resource_uid:dr359 OR data_resource_uid:dr571 OR data_resource_uid:dr570)\",\n" +
            "\"ROLE_SDS_NZ\" : \"sensitive:\\\"generalised\\\" AND (data_resource_uid:dr2707 OR data_resource_uid:dr812 OR data_resource_uid:dr814 OR data_resource_uid:dr808 OR data_resource_uid:dr806 OR data_resource_uid:dr815 OR data_resource_uid:dr802 OR data_resource_uid:dr805 OR data_resource_uid:dr813) AND -cl927:* AND -(data_resource_uid:dr359 OR data_resource_uid:dr571 OR data_resource_uid:dr570)\",\n" +
            "\"ROLE_SDS_NT\" : \"sensitive:\\\"generalised\\\" AND cl927:\\\"Northern Territory (including Coastal Waters)\\\" AND -(data_resource_uid:dr359 OR data_resource_uid:dr571 OR data_resource_uid:dr570)\",\n" +
            "\"ROLE_SDS_QLD\" : \"sensitive:\\\"generalised\\\" AND cl927:\\\"Queensland (including Coastal Waters)\\\" AND -(data_resource_uid:dr359 OR data_resource_uid:dr571 OR data_resource_uid:dr570)\",\n" +
            "\"ROLE_SDS_SA\" : \"sensitive:\\\"generalised\\\" AND cl927:\\\"South Australia (including Coastal Waters)\\\" AND -(data_resource_uid:dr359 OR data_resource_uid:dr571 OR data_resource_uid:dr570)\",\n" +
            "\"ROLE_SDS_TAS\" : \"sensitive:\\\"generalised\\\" AND cl927:\\\"Tasmania (including Coastal Waters)\\\" AND -(data_resource_uid:dr359 OR data_resource_uid:dr571 OR data_resource_uid:dr570)\",\n" +
            "\"ROLE_SDS_VIC\" : \"sensitive:\\\"generalised\\\" AND cl927:\\\"Victoria (including Coastal Waters)\\\" AND -(data_resource_uid:dr359 OR data_resource_uid:dr571 OR data_resource_uid:dr570)\",\n" +
            "\"ROLE_SDS_WA\" : \"sensitive:\\\"generalised\\\" AND cl927:\\\"Western Australia (including Coastal Waters)\\\" AND -(data_resource_uid:dr359 OR data_resource_uid:dr571 OR data_resource_uid:dr570)\",\n" +
            "\"ROLE_SDS_BIRDLIFE\" : \"sensitive:\\\"generalised\\\" AND (data_resource_uid:dr359 OR data_resource_uid:dr571 OR data_resource_uid:dr570)\"\n" +
            "\n" +
            "}}")
    protected String sensitiveAccessRoles = "{}";;

    private JSONObject sensitiveAccessRolesToSolrFilters;

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

    @Value("${download.date.format:EEE MMM dd HH:mm:ss z yyyy}")
    public String downloadDateFormat = "EEE MMM dd HH:mm:ss z yyyy";

    @Value("${download.shp.enabled:true}")
    public void setDownloadShpEnabled(Boolean downloadShpEnabled) {
        DownloadService.downloadShpEnabled = downloadShpEnabled;
    }

    public static Boolean downloadShpEnabled;

    /**
     * Ensures closure is only attempted once.
     */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Ensures initialisation is only attempted once, to avoid creating too many threads.
     */
    private final AtomicBoolean initialised = new AtomicBoolean(false);

    /**
     * A latch that is released once initialisation completes, to enable the off-thread
     * initialisation to occur completely before servicing queries.
     */
    private final CountDownLatch initialisationLatch = new CountDownLatch(1);

    /**
     * Call this method at the start of web service calls that require initialisation to be complete before continuing.
     * This blocks until it is either interrupted or the initialisation thread from {@link #init()} is finished (successful or not).
     */
    protected final void afterInitialisation() {
        try {
            initialisationLatch.await();
        } catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private final Queue<Thread> runningDownloadControllers = new LinkedBlockingQueue<>();
    private final Queue<DownloadControlThread> runningDownloadControlRunnables = new LinkedBlockingQueue<>();

    private volatile ExecutorService offlineParallelQueryExecutor;

    @PostConstruct
    public void init() throws ParseException {

        // Simple JSON initialisation, let's follow the default Spring semantics
        sensitiveAccessRolesToSolrFilters = (JSONObject) new JSONParser().parse(sensitiveAccessRoles);

        if(initialised.compareAndSet(false, true)) {
            //init on thread so as to not hold up other PostConstruct that this may depend on
            new Thread() {
                @Override
                public void run() {
                    try
                    {
                        ExecutorService nextParallelExecutor = getOfflineThreadPoolExecutor();
                        // Create the implementation for the threads running in the DownloadControlThread
                        DownloadCreator nextDownloadCreator = getNewDownloadCreator();
                        // Create executors based on the concurrent.downloads.json property
                        try {
                            JSONParser jp = new JSONParser();
                            JSONArray concurrentDownloadsJsonArray = (JSONArray) jp.parse(concurrentDownloadsJSON);
                            for (Object o : concurrentDownloadsJsonArray) {
                                JSONObject jo = (JSONObject) o;
                                int threads = ((Long) jo.get("threads")).intValue();
                                Integer maxRecords = jo.containsKey("maxRecords") ? ((Long) jo.get("maxRecords")).intValue() : null;
                                String type = jo.containsKey("type") ? jo.get("type").toString() : null;
                                String label = jo.containsKey("label") ? jo.get("label").toString() + "-" : "";
                                Long pollDelayMs = jo.containsKey("pollDelay") ? (Long) jo.get("pollDelay") : null;
                                Long executionDelayMs = jo.containsKey("executionDelay") ? (Long) jo.get("executionDelay") : null;
                                Integer threadPriority = jo.containsKey("threadPriority") ? ((Long) jo.get("threadPriority")).intValue() : Thread.NORM_PRIORITY;
                                DownloadType dt = null;
                                if (type != null) {
                                    dt = "index".equals(type) ? DownloadType.RECORDS_INDEX : DownloadType.RECORDS_DB;
                                }

                                String nextThreadName = "biocache-download-control-";
                                nextThreadName += label;
                                nextThreadName += (maxRecords == null ? "nolimit" : maxRecords.toString()) + "-";
                                nextThreadName += (dt == null ? "alltypes" : dt.name()) + "-";
                                nextThreadName += "poolsize-" + threads;

                                DownloadControlThread nextRunnable = new DownloadControlThread(nextThreadName, maxRecords, dt, threads, pollDelayMs, executionDelayMs, threadPriority, currentDownloads, nextDownloadCreator, persistentQueueDAO, nextParallelExecutor);
                                Thread nextThread = new Thread(nextRunnable);
                                nextThread.setName(nextThreadName);
                                // Control threads need to wakeup regularly to check for new downloads
                                nextThread.setPriority(Thread.NORM_PRIORITY + 1);
                                runningDownloadControllers.add(nextThread);
                                runningDownloadControlRunnables.add(nextRunnable);
                                nextThread.start();
                            }
                        } catch (Exception e) {
                            logger.error("Failed to create all extra offline download threads for concurrent.downloads.extra=" + concurrentDownloadsJSON, e);
                        }
                        // If no threads were created, then add a single default thread
                        if(runningDownloadControllers.isEmpty()) {
                            logger.error("No offline download threads were created from configuration, creating a single default download thread instead.");
                            DownloadControlThread nextRunnable = new DownloadControlThread(
                                    null,
                                    null,
                                    DownloadType.RECORDS_INDEX,
                                    1,
                                    0L,
                                    0L,
                                    Thread.NORM_PRIORITY,
                                    currentDownloads,
                                    nextDownloadCreator,
                                    persistentQueueDAO,
                                    nextParallelExecutor
                            );
                            Thread nextThread = new Thread(nextRunnable);
                            String nextThreadName = "biocache-download-control-";
                            nextThreadName += "defaultNoConfigFound-";
                            nextThreadName += "nolimit-";
                            nextThreadName += "alltypes-";
                            nextThreadName += "poolsize-1";
                            nextThread.setName(nextThreadName);
                            // Control threads need to wakeup regularly to check for new downloads
                            nextThread.setPriority(Thread.NORM_PRIORITY + 1);
                            runningDownloadControllers.add(nextThread);
                            runningDownloadControlRunnables.add(nextRunnable);
                            nextThread.start();
                        }
                    } finally {
                        initialisationLatch.countDown();
                    }
                }

            }.start();
        }
    }

    /**
     * Overridable method called during the intialisation phase to customise the DownloadCreator implementation
     * used by the DownloadService, particularly for testing.
     *
     * @return A new instance of DownloadCreator to be used by {@link DownloadControlThread} instances.
     */
    protected DownloadCreator getNewDownloadCreator() {
        return new DownloadCreatorImpl();
    }

    /**
     * @return An instance of ExecutorService used to concurrently execute parallel queries for offline downloads.
     */
    private ExecutorService getOfflineThreadPoolExecutor() {
        ExecutorService nextExecutor = offlineParallelQueryExecutor;
        if(nextExecutor == null){
            synchronized(this) {
                nextExecutor = offlineParallelQueryExecutor;
                if(nextExecutor == null) {
                    nextExecutor = offlineParallelQueryExecutor = Executors.newFixedThreadPool(
                                                                getMaxOfflineParallelDownloadThreads(),
                                                                new ThreadFactoryBuilder().setNameFormat("biocache-query-offline-%d")
                                                                .setPriority(Thread.MIN_PRIORITY).build());
                }
            }
        }
        return nextExecutor;
    }

    private int getMaxOfflineParallelDownloadThreads() {
        return maxOfflineParallelQueryDownloadThreads;
    }

    /**
     * Ensures that all of the download threads are given a chance to shutdown cleanly using thread interrupts when a Spring {@link ContextClosedEvent} occurs.
     */
    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        afterInitialisation();
        if(closed.compareAndSet(false, true)) {
            try {
                // Stop more downloads from being added by shutting down additions to the persistent queue
                persistentQueueDAO.shutdown();
            }
            finally {
                DownloadControlThread nextToCloseRunnable = null;
                // Call a non-blocking shutdown command on all of the download control threads
                while((nextToCloseRunnable = runningDownloadControlRunnables.poll()) != null) {
                    nextToCloseRunnable.shutdown();
                }

                // Give threads a chance to react to the shutdown flag before checking if they are alive
                try {
                    Thread.sleep(2000);
                }
                catch(InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                Thread nextToCloseThread = null;
                List<Thread> toJoinThreads = new ArrayList<>();
                while((nextToCloseThread = runningDownloadControllers.poll()) != null) {
                    if(nextToCloseThread.isAlive()) {
                        toJoinThreads.add(nextToCloseThread);
                    }
                }

                if(!toJoinThreads.isEmpty()) {
                    // Give remaining download control threads a few seconds to cleanup before interrupting
                    try {
                        Thread.sleep(5000);
                    }
                    catch(InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }

                    for(final Thread nextToJoinThread : toJoinThreads) {
                        if(nextToJoinThread.isAlive()) {
                            // Interrupt any threads that are still alive after the non-blocking shutdown command
                            nextToJoinThread.interrupt();
                        }
                    }
                }
            }
        }
    }

    /**
     * Registers a new active download
     *
     * @param requestParams
     * @param ip
     * @param type
     * @return
     */
    public DownloadDetailsDTO registerDownload(DownloadRequestParams requestParams, String ip, String userAgent,
            DownloadDetailsDTO.DownloadType type) {
        afterInitialisation();
        DownloadDetailsDTO dd = new DownloadDetailsDTO(requestParams, ip, userAgent, type);
        dd.setRequestParams(requestParams);
        currentDownloads.add(dd);
        return dd;
    }

    /**
     * Removes a completed download from active list.
     *
     * @param dd
     */
    public void unregisterDownload(DownloadDetailsDTO dd) {
        afterInitialisation();
        // remove it from the list
        try {
            currentDownloads.remove(dd);
        } finally {
            persistentQueueDAO.removeDownloadFromQueue(dd);
        }
    }

    /**
     * Returns a list of current downloads
     *
     * @return
     */
    public List<DownloadDetailsDTO> getCurrentDownloads() {
        afterInitialisation();
        List<DownloadDetailsDTO> result = new ArrayList<>(currentDownloads);
        return Collections.unmodifiableList(result);
    }

    /**
     * Writes the supplied download to the supplied output stream. It will
     * include all the appropriate citations etc.
     * 
     * @param dd
     * @param requestParams
     * @param ip
     * @param out
     * @param includeSensitive
     * @param fromIndex
     * @throws Exception
     * @deprecated Use {@link #writeQueryToStream(DownloadDetailsDTO, DownloadRequestParams, String, OutputStream, boolean, boolean, boolean, boolean, ExecutorService, List)} instead.
     */
    @Deprecated
    public void writeQueryToStream(DownloadDetailsDTO dd, DownloadRequestParams requestParams, String ip,
                                   OutputStream out, boolean includeSensitive, boolean fromIndex, boolean limit, boolean zip)
            throws Exception {
        afterInitialisation();

        writeQueryToStream(dd, requestParams, ip, out, includeSensitive, fromIndex, limit, zip, getOfflineThreadPoolExecutor(), null);
    }

    /**
     * Writes the supplied download to the supplied output stream. It will
     * include all the appropriate citations etc.
     *
     * @param dd
     * @param requestParams
     * @param ip
     * @param out
     * @param includeSensitive
     * @param fromIndex
     * @param doiResponseList Return the CreateDoiResponse instance as the first element of the list if requestParams.mintDoi was true
     * @throws Exception
     */
    public void writeQueryToStream(DownloadDetailsDTO dd, DownloadRequestParams requestParams, String ip,
                                   OutputStream out, boolean includeSensitive, boolean fromIndex, boolean limit, boolean zip, ExecutorService parallelExecutor, List<CreateDoiResponse> doiResponseList)
            throws Exception {
        afterInitialisation();
        String filename = requestParams.getFile();
        String originalParams = requestParams.toString();

        // Use a zip output stream to include the data and citation together in
        // the download.
        // Note: When producing a shp the output will stream a csv followed by a zip.
        try(OptionalZipOutputStream sp = new OptionalZipOutputStream(
                zip ? OptionalZipOutputStream.Type.zipped : OptionalZipOutputStream.Type.unzipped, new CloseShieldOutputStream(out), maxMB);) {
            String suffix = requestParams.getFileType().equals("shp") ? "csv" : requestParams.getFileType();
            sp.putNextEntry(filename + "." + suffix);
            // put the facets
            if ("all".equals(requestParams.getQa())) {
                requestParams.setFacets(new String[] { "assertions", "data_resource_uid" });
            } else {
                requestParams.setFacets(new String[] { "data_resource_uid" });
            }
            
            final ConcurrentMap<String, AtomicInteger> uidStats;
            if (fromIndex) {
                uidStats = searchDAO.writeResultsFromIndexToStream(requestParams, sp, includeSensitive, dd, limit, parallelExecutor);
            } else {
                uidStats = searchDAO.writeResultsToStream(requestParams, sp, 100, includeSensitive, dd, limit);
            }

            sp.closeEntry();

            // add the readme for the Shape file header mappings if necessary
            if (dd.getHeaderMap() != null) {
                sp.putNextEntry("Shape-README.html");
                sp.write(
                        ("The name of features is limited to 10 characters. Listed below are the mappings of feature name to download field:")
                                .getBytes(StandardCharsets.UTF_8));
                sp.write(("<table><td><b>Feature</b></td><td><b>Download Field<b></td>").getBytes(StandardCharsets.UTF_8));
                for (String key : dd.getHeaderMap().keySet()) {
                    sp.write(("<tr><td>" + key + "</td><td>" + dd.getHeaderMap().get(key) + "</td></tr>").getBytes(StandardCharsets.UTF_8));
                }
                sp.write(("</table>").getBytes(StandardCharsets.UTF_8));
            }

            if (uidStats != null) {
                // Add the data citation to the download
                List<String> citationsForReadme = new ArrayList<String>();

                Boolean mintDoi = requestParams.getMintDoi();
                CreateDoiResponse doiResponse = null;
                String doi = "";

                final String searchUrl = generateSearchUrl(requestParams);

                List<QualityFilterDTO> qualityFilters = getQualityFilterDTOS(requestParams);

                if (citationsEnabled) {
                    List<Map<String, String>> datasetMetadata = null;
                    if(mintDoi) {
                        datasetMetadata = new ArrayList<>();
                    }

                    // add the citations for the supplied uids
                    sp.putNextEntry("citation.csv");
                    try {
                        getCitations(uidStats, sp, requestParams.getSep(), requestParams.getEsc(), citationsForReadme, datasetMetadata);
                    } catch (IOException e) {
                        logger.error(e.getMessage(), e);
                    }
                    sp.closeEntry();


                    if(mintDoi) {

                        Map<String, ?> userDetails = authService.getUserDetails(dd.getEmail());

                        //Source requester details
                        String requesterId = (String) userDetails.get("userId");
                        String requesterName = userDetails.get("firstName") + " " + userDetails.get("lastName");

                        // Prepare licence
                        Set<String> datasetLicences = new TreeSet<>();
                        for(Map<String, String> dataset: datasetMetadata) {
                            String licence = dataset.get("licence");

                            if(StringUtils.isNotBlank(licence) ) {
                                datasetLicences.add(licence);
                            }
                        }

                        List<String> licence = Lists.newArrayList(datasetLicences);

                        try {


                            DownloadDoiDTO doiDetails = new DownloadDoiDTO();

                            doiDetails.setTitle(biocacheDownloadDoiTitlePrefix + filename);
                            doiDetails.setApplicationUrl(searchUrl);
                            doiDetails.setRequesterId(requesterId);
                            if (dd.getSensitiveFq() != null) {
                                doiDetails.setAuthorisedRoles(getSensitiveRolesForUser(requesterId));
                            }

                            doiDetails.setRequesterName(requesterName);
                            doiDetails.setDatasetMetadata(datasetMetadata);
                            doiDetails.setRequestTime(dd.getStartDateString());
                            doiDetails.setRecordCount(dd.getTotalRecords());
                            doiDetails.setLicence(licence);
                            doiDetails.setQueryTitle(requestParams.getDisplayString());
                            doiDetails.setApplicationMetadata(requestParams.getDoiMetadata());
                            doiDetails.setQualityFilters(qualityFilters);
                            doiResponse = doiService.mintDoi(doiDetails);

                        } catch (Exception e) {
                            logger.error("DOI minting failed", e);
                        }
                        if(doiResponse != null) {
                            logger.debug("DOI minted: " + doiResponse.getDoi());
                            doiResponseList.add (doiResponse);
                        } else {
                            logger.error("DOI minting failed for path " + dd.getFileLocation());
                        }
                    }
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Not adding citation. Enabled: " + citationsEnabled + " uids: " + uidStats);
                    }
                }

                // online downloads will not have a file location or request params set
                // in dd.
                if (dd.getRequestParams() == null) {
                    dd.setRequestParams(requestParams);
                }
                if (dd.getFileLocation() == null) {
                    dd.setFileLocation(searchUrl);
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
                        // final String doiLandingPage = requestParams.getDoiDisplayUrl() != null ? requestParams.getDoiDisplayUrl() : biocacheDownloadDoiLandingPage;
                        // fileLocation = doiLandingPage + doi;
                        fileLocation = OFFICIAL_DOI_RESOLVER + doi;

                    } else {
                        readmeFile = biocacheDownloadReadmeTemplate;
                        fileLocation = dd.getFileLocation().replace(biocacheDownloadDir, biocacheDownloadUrl);
                    }

                    String readmeTemplate = "";
                    if (new File(readmeFile).exists()) {
                        readmeTemplate = Files.asCharSource(new File(readmeFile), StandardCharsets.UTF_8).read();
                    }

                    String dataQualityFilters = "";
                    if (!qualityFilters.isEmpty()) {
                        dataQualityFilters = getDataQualityFiltersString(qualityFilters);
                    }

                    String readmeContent = readmeTemplate.replace("[url]", fileLocation)
                            .replace("[date]", dd.getStartDateString(downloadDateFormat))
                            .replace("[searchUrl]", searchUrl)
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
                        getHeadings(uidStats, sp, requestParams, dd.getMiscFields());
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                    sp.closeEntry();
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Not adding header. Enabled: " + headingsEnabled + " uids: " + uidStats);
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

                // remove header entries from uidStats
                List<String> toRemove = new ArrayList<String>();
                for (String key : uidStats.keySet()) {
                    if (uidStats.get(key).get() < 0) {
                        toRemove.add(key);
                    }
                }
                for (String key : toRemove) {
                    uidStats.remove(key);
                }

                // log the stats to ala logger
                LogEventVO vo = new LogEventVO(1002, requestParams.getReasonTypeId(), requestParams.getSourceTypeId(),
                        requestParams.getEmail(), requestParams.getReason(), ip, dd.getUserAgent(), null, uidStats, sourceUrl);

                loggerService.logEvent(vo);
//                logger.log(RestLevel.REMOTE, vo);

            }
        } catch (RecordWriterException e) {
            logger.error(e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // sApplication may be shutting down, do not delete the download file
            throw e;
        }
    }

    @VisibleForTesting
    List<QualityFilterDTO> getQualityFilterDTOS(DownloadRequestParams requestParams) {
        List<QualityFilterDTO> qualityFilters = new ArrayList<>();
        if (requestParams.getQualityFiltersInfo() != null) {
            List<String> filters = requestParams.getQualityFiltersInfo();
            filters.forEach( filter -> {
                String[] strings = split(filter, ":", 2);
                if (strings.length == 2) {
                    QualityFilterDTO dto = new QualityFilterDTO(strings[0], strings[1]);
                    qualityFilters.add(dto);
                }
            });
        }
        return qualityFilters;
    }

    @VisibleForTesting
    String getDataQualityFiltersString(List<QualityFilterDTO> qualityFilters) throws IOException, org.apache.velocity.runtime.parser.ParseException {
        String dataQualityFilters;
        RuntimeServices runtimeServices = RuntimeSingleton.getRuntimeServices();
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
     *
     * @param requestParams
     * @param response
     * @param ip
     * @param out
     * @param includeSensitive
     * @param fromIndex
     * @param zip
     * @throws Exception
     * @deprecated Use {@link #writeQueryToStream(DownloadRequestParams, HttpServletResponse, String, String, OutputStream, boolean, boolean, boolean, ExecutorService)} instead.
     */
    @Deprecated
    public void writeQueryToStream(DownloadRequestParams requestParams, HttpServletResponse response, String ip, String userAgent,
            OutputStream out, boolean includeSensitive, boolean fromIndex, boolean zip) throws Exception {
        afterInitialisation();
        writeQueryToStream(requestParams, response, ip, userAgent, out, includeSensitive, fromIndex, zip, getOfflineThreadPoolExecutor());
    }


    public void writeQueryToStream(DownloadRequestParams requestParams, HttpServletResponse response, String ip, String userAgent,
            OutputStream out, boolean includeSensitive, boolean fromIndex, boolean zip, ExecutorService parallelQueryExecutor) throws Exception {
        afterInitialisation();
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

        DownloadDetailsDTO.DownloadType type = fromIndex ? DownloadType.RECORDS_INDEX : DownloadType.RECORDS_DB;
        DownloadDetailsDTO dd = registerDownload(requestParams, ip, userAgent, type);
        writeQueryToStream(dd, requestParams, ip, new CloseShieldOutputStream(out), includeSensitive, fromIndex, true, zip, parallelQueryExecutor, null);
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
            afterInitialisation();

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
                writer.writeNext(new String[] { messageSource.getMessage("citation.uid", null, "UID", null),
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
                    final int UID=0;
                    final int NAME=1;
                    final int CITATION=3;
                    final int RIGHTS=4;
                    final int LINK=5;
                    final int COUNT=9;
                    for (Map<String, Object> record : entities) {
                        // ensure that the record is not null to prevent NPE on
                        // the "get"s
                        if (record != null) {
                            Object value = record.get("uid");
                            if(value != null) {
                                AtomicInteger uidRecordCount = uidStats.get(value);
                                String count = Optional.ofNullable(uidRecordCount).orElseGet(() -> new AtomicInteger(0)).toString();
                                String[] row = new String[] {
                                        getOrElse(record, "uid", ""),
                                        getOrElse(record, "name", ""),
                                        getOrElse(record, "DOI", ""),
                                        getOrElse(record, "citation", ""),
                                        getOrElse(record, "rights", ""),
                                        getOrElse(record, "link", ""),
                                        getOrElse(record, "dataGeneralizations", ""),
                                        getOrElse(record, "informationWithheld", ""),
                                        getOrElse(record, "downloadLimit", ""),
                                        count };
                                writer.writeNext(row);

                                if (readmeCitations != null) {
                                    // used in README.txt
                                    readmeCitations.add(row[CITATION] + " (" + row[RIGHTS] + "). " + row[LINK]);
                                }

                                if (datasetMetadata != null ) {
                                    Map<String,String> dataSet = new HashMap<>();

                                    dataSet.put("uid", row[UID]);
                                    dataSet.put("name", row[NAME]);
                                    dataSet.put("licence", row[RIGHTS]);
                                    dataSet.put("count", row[COUNT]);

                                    datasetMetadata.add(dataSet);
                                }
                            } else {
                                logger.error("Record did not have a uid attribute: " + record);
                            }
                        } else {
                            logger.error("A null record was returned from the collectory citation service: " + entities + ", collected stats were: " + uidStats);
                        }
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
     * 
     * output columns: column name field requested dwc description info field
     *
     * @param out
     * @throws HttpException
     * @throws IOException
     */
    public void getHeadings(ConcurrentMap<String, AtomicInteger> uidStats, OutputStream out,
            DownloadRequestParams params, String[] miscHeaders) throws Exception {
        if (headingsEnabled) {
            afterInitialisation();

            if (out == null) {
                logger.error("Unable to generate headings info: output stream was null", new Exception().fillInStackTrace());
                return;
            }

            try (CSVWriter writer = new CSVWriter(new OutputStreamWriter(new CloseShieldOutputStream(out), StandardCharsets.UTF_8), params.getSep(), '"',
                    params.getEsc());) {
                // Object[] citations =
                // restfulClient.restPost(citationServiceUrl, "text/json",
                // uidStats.keySet());
                Set<IndexFieldDTO> indexedFields = searchDAO.getIndexedFields();

                // header
                writer.writeNext(new String[] { "Column name", "Requested field", "DwC Name", "Field name",
                        "Field description", "Download field name", "Download field description", "More information" });

                String[] fieldsRequested = null;
                String[] headerOutput = null;
                for (Map.Entry<String, AtomicInteger> e : uidStats.entrySet()) {
                    if (e.getValue().get() == -1) {
                        // String fields requested
                        fieldsRequested = e.getKey().split(",");
                    } else if (e.getValue().get() == -2) {
                        headerOutput = e.getKey().split(",");
                    }
                }

                if (fieldsRequested != null && headerOutput != null) {
                    // ignore first fieldsRequested and headerOutput record
                    for (int i = 1; i < fieldsRequested.length; i++) {

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
                            writer.writeNext(new String[] { headerOutput[i], fieldsRequested[i],
                                    ifdto.getDwcTerm() != null ? ifdto.getDwcTerm() : "",
                                    ifdto.getName() != null ? ifdto.getName() : "",
                                    ifdto.getDescription() != null ? ifdto.getDescription() : "",
                                    ifdto.getDownloadName() != null ? ifdto.getDownloadName() : "",
                                    ifdto.getDownloadDescription() != null ? ifdto.getDownloadDescription() : "",
                                    ifdto.getInfo() != null ? ifdto.getInfo() : "" });
                        } else if (StringUtils.isNotEmpty(headerOutput[i])){
                            // others, e.g. assertions
                            String info = messageSource.getMessage("description." + fieldsRequested[i], null, "", null);
                            writer.writeNext(new String[] { headerOutput[i], fieldsRequested[i], "", "", "", "", "",
                                    info != null ? info : "" });
                        }
                    }
                }

                // misc headers
                if (miscHeaders != null) {
                    String defaultDescription = messageSource.getMessage("description.", null, "Raw field from data provider.", null);
                    for (int i = 0; i < miscHeaders.length; i++) {
                        writer.writeNext(
                                new String[] { miscHeaders[i], "", "", "", "", "", messageSource.getMessage("description." + miscHeaders[i], null, defaultDescription, null)});
                    }
                }

                writer.flush();
            }
        }
    }

    private String getOrElse(Map<String, Object> map, String key, String defaultValue) {
        return map.getOrDefault(key, defaultValue).toString();
    }

    /**
     * Generate a search URL the user can use to regenerate the same download
     * (assumes they came via biocache UI)
     *
     * @param params
     * @return url
     */
    public String generateSearchUrl(DownloadRequestParams params) {
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
     * @param userId The user the filter is built for
     * @return A String with a Solr filter
     */
    public String getSensitiveFq(String userId) {

        if (downloadAuthSensitive == null || !downloadAuthSensitive) {
            return null;
        }

        String sensitiveFq = "";

        for (String sensitiveRole : getSensitiveRolesForUser(userId)) {
            if (sensitiveFq.length() > 0) {
                sensitiveFq += " OR ";
            }
            sensitiveFq += "(" + sensitiveAccessRolesToSolrFilters.get(sensitiveRole) + ")";
        }

        if (sensitiveFq.length() == 0) {
            return null;
        }

        if(logger.isDebugEnabled()) {
            logger.debug("sensitiveOnly download requested for user: " + userId +
                    ", using fq: " + sensitiveFq);
        }

        return sensitiveFq;
    }

    /**
     * List the sensitive roles for a given user
     * @param userId The user
     * @return The sensitive roles for the user, the list will be empty if the user has no sensitive roles
     */
    public List<String> getSensitiveRolesForUser(String userId) {
        List<String> userRoles = authService.getUserRoles(userId);

        List<String> result = new ArrayList<>(sensitiveAccessRolesToSolrFilters.keySet());

        result.retainAll(userRoles);

        return result;
    }


    private class DownloadCreatorImpl implements DownloadCreator {
        @Override
        public Callable<DownloadDetailsDTO> createCallable(final DownloadDetailsDTO currentDownload, final long executionDelay, final Semaphore capacitySemaphore, final ExecutorService parallelExecutor) {
            return new Callable<DownloadDetailsDTO>() {

                @Override
                public DownloadDetailsDTO call() throws Exception {
                    try {
                        if(logger.isInfoEnabled()) {
                            logger.info("Starting to download the offline request: " + currentDownload);
                        }
                        Thread.sleep(executionDelay);
                        // we are now ready to start the download
                        // we need to create an output stream to the file system

                        boolean shuttingDown = false;
                        boolean doRetry = false;

                        try (FileOutputStream fos = FileUtils
                                .openOutputStream(new File(currentDownload.getFileLocation()));) {
                            // cannot include misc columns if shp
                            if (!currentDownload.getRequestParams().getFileType().equals("csv")
                                    && currentDownload.getRequestParams().getIncludeMisc()) {
                                currentDownload.getRequestParams().setIncludeMisc(false);
                            }

                            List<CreateDoiResponse> doiResponseList = null;
                            Boolean mintDoi = currentDownload.getRequestParams().getMintDoi();

                            String doiFailureMessage = "";
                            if(mintDoi) {
                                doiResponseList = new ArrayList<>();
                            }
                            writeQueryToStream(currentDownload, currentDownload.getRequestParams(),
                                    currentDownload.getIpAddress(), new CloseShieldOutputStream(fos), currentDownload.getIncludeSensitive(),
                                    currentDownload.getDownloadType() == DownloadType.RECORDS_INDEX, false, true, parallelExecutor, doiResponseList);

                            if(mintDoi && doiResponseList.size() <= 0) {
                                //DOI Minting failed
                                doiFailureMessage = biocacheDownloadDoiFailureMessage;
                                mintDoi = false; //Prevent any updates
                            }

                            // now that the download is complete email a link to the
                            // recipient.
                            final String hubName = currentDownload.getRequestParams().getHubName() != null ? currentDownload.getRequestParams().getHubName() : "ALA";
                            String subject = messageSource.getMessage("offlineEmailSubject", null, biocacheDownloadEmailSubject, null)
                                    .replace("[filename]", currentDownload.getRequestParams().getFile())
                                    .replace("[hubName]",hubName);

                            if (currentDownload != null && currentDownload.getFileLocation() != null) {
                                insertMiscHeader(currentDownload);

                                //ensure new directories have correct permissions
                                new File(currentDownload.getFileLocation()).getParentFile().setExecutable(true, false);
                                new File(currentDownload.getFileLocation()).getParentFile().getParentFile().setExecutable(true, false);


                                String archiveFileLocation = biocacheDownloadUrl + File.separator + URLEncoder.encode(currentDownload.getFileLocation().replace(biocacheDownloadDir + "/",""), "UTF-8").replace("%2F", "/").replace("+", "%20");

                                String doiStr = "";
                                String emailBody;
                                String emailTemplate;
                                String downloadFileLocation;
                                String officialFileLocation = "";
                                if(mintDoi && doiResponseList != null && !doiResponseList.isEmpty() && doiResponseList.get(0) != null) {

                                    CreateDoiResponse doiResponse;
                                    doiResponse = doiResponseList.get(0);
                                    try {
                                        doiService.updateFile(doiResponse.getUuid(), currentDownload.getFileLocation());
                                        doiStr = doiResponse.getDoi();
                                        emailTemplate = biocacheDownloadDoiEmailTemplate;

                                        // TODO: The downloads-plugin has issues with unencoded user queries 
                                        // Working around that by hardcoding the official DOI resolution service as the landing page
                                        // https://github.com/AtlasOfLivingAustralia/biocache-service/issues/311
                                        //final String doiLandingPage = currentDownload.getRequestParams().getDoiDisplayUrl() != null ? currentDownload.getRequestParams().getDoiDisplayUrl() : biocacheDownloadDoiLandingPage;
                                        //downloadFileLocation = doiLandingPage + doiStr;
                                        downloadFileLocation = alaDoiResolver + doiStr;
                                        officialFileLocation = OFFICIAL_DOI_RESOLVER + doiStr;
                                    }
                                    catch (Exception ex) {
                                        logger.error("DOI update failed for DOI uuid " + doiResponse.getUuid() +
                                                " and path " + currentDownload.getFileLocation(), ex);
                                        doiFailureMessage = biocacheDownloadDoiFailureMessage;
                                        emailTemplate = biocacheDownloadEmailTemplate;
                                        downloadFileLocation = archiveFileLocation;
                                    }
                                } else {
                                    emailTemplate = biocacheDownloadEmailTemplate;
                                    downloadFileLocation = archiveFileLocation;
                                }

                                if (currentDownload.isEmailNotify()) {

                                    emailBody = Files.asCharSource(new File(emailTemplate), StandardCharsets.UTF_8).read();

                                    final String searchUrl = generateSearchUrl(currentDownload.getRequestParams());
                                    String emailBodyHtml = emailBody.replace("[url]", downloadFileLocation)
                                            .replace("[officialDoiUrl]", officialFileLocation)
                                            .replace("[date]", currentDownload.getStartDateString(downloadDateFormat))
                                            .replace("[searchUrl]", searchUrl)
                                            .replace("[queryTitle]", currentDownload.getRequestParams().getDisplayString())
                                            .replace("[doiFailureMessage]", doiFailureMessage);
                                    String body = messageSource.getMessage("offlineEmailBody",
                                            new Object[]{archiveFileLocation, searchUrl, currentDownload.getStartDateString(downloadDateFormat)},
                                            emailBodyHtml, null);

                                    // save the statistics to the download directory
                                    try (FileOutputStream statsStream = FileUtils
                                            .openOutputStream(new File(new File(currentDownload.getFileLocation()).getParent()
                                                    + File.separator + "downloadStats.json"))) {
                                        objectMapper.writeValue(statsStream, currentDownload);
                                    }

                                    if(mintDoi && doiResponseList != null && !doiResponseList.isEmpty() && doiResponseList.get(0) != null) {
                                        // Delay sending the email to allow the DOI to propagate through to upstream DOI providers
                                        Thread.sleep(doiPropagationDelay);
                                    }

                                    emailService.sendEmail(currentDownload.getEmail(), subject, body);
                                }
                            }

                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            //shutting down
                            shuttingDown = true;
                            throw e;
                        } catch (CancellationException e) {
                            //download cancelled, do not send an email
                        } catch (com.datastax.driver.core.exceptions.DriverException e) {
                            logger.warn("Offline download failed. Cassandra driver error. Retrying in 5 mins. Task file: " + currentDownload.getFileLocation() + " : " + e.getMessage(), e);
                            //return to queue in 5mins as long as the VM is not restarted during that time, in which case it could be permanently stuck in a broken state
                            doRetry = true;
                            newRetryThread(currentDownload).start();
                        } catch (PelopsException e) {
                            logger.warn("Offline download failed. Cassandra error. Retrying in 5 mins. Task file: " + currentDownload.getFileLocation() + " : " + e.getMessage(), e);
                            //return to queue in 5mins as long as the VM is not restarted during that time, in which case it could be permanently stuck in a broken state
                            doRetry = true;
                            newRetryThread(currentDownload).start();
                        } catch (Exception e) {
                            logger.error("Error in offline download, sending email. download path: "
                                    + currentDownload.getFileLocation(), e);

                            try {
                                final String hubName = currentDownload.getRequestParams().getHubName() != null ? currentDownload.getRequestParams().getHubName() : "ALA";
                                String subject = messageSource.getMessage("offlineEmailSubjectError", null, biocacheDownloadEmailSubjectError, null)
                                        .replace("[filename]", currentDownload.getRequestParams().getFile())
                                        .replace("[hubName]",hubName);

                                String fileLocation = currentDownload.getFileLocation().replace(biocacheDownloadDir,
                                        biocacheDownloadUrl);
                                String body = messageSource.getMessage("offlineEmailBodyError",
                                        new Object[] { fileLocation },
                                        biocacheDownloadEmailBodyError.replace("[url]", fileLocation)
                                                .replace("[hubName]", hubName), null);
                                String searchUrl = generateSearchUrl(currentDownload.getRequestParams());
                                String copyTo = supportEmailEnabled ? supportEmail : null;
                                // email error to user and support (configurable)
                                emailService.sendEmail(currentDownload.getEmail(), copyTo, subject,
                                        body + " Please contact " + support + " by replying to this email and we will investigate the cause."
                                                + "<br><br>Your search URL was: <br>" + searchUrl
                                                + "<br><br>The reference to quote is:<br>" + currentDownload.getUniqueId()
                                                + "<br><br>Your successful downloads can be found at:<br>"
                                                + myDownloadsUrl);
                            } catch (Exception ex) {
                                logger.error("Error sending error message to download email. "
                                        + currentDownload.getFileLocation(), ex);
                            }
                        } finally {
                            // in case of server up/down, only remove from queue
                            // after emails are sent
                            if (!shuttingDown && !doRetry) {
                                unregisterDownload(currentDownload);
                            }
                        }
                        return currentDownload;
                    } finally {
                        capacitySemaphore.release();
                    }
                }
            };
        }
    }

    private Thread newRetryThread(final DownloadDetailsDTO currentDownload) {
        return new Thread() {
            @Override
            public void run() {
                boolean showErrorMessage = true;
                try {
                    Thread.sleep(5*60*1000);
                    try {
                        FileUtils.deleteDirectory(new File(currentDownload.getFileLocation()).getParentFile());
                    } catch (IOException e) {
                        logger.error("Exception when attempting to delete failed download " +
                                "directory before retrying: " + new File(currentDownload.getFileLocation()).getParent() +
                                ", " + e.getMessage(), e);
                    }
                    currentDownload.setFileLocation(null);
                    // If we get successfully to this line, do not log an error message
                    showErrorMessage = false;
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                    logger.error("Interrupted while waiting to delete failed download: directory before retrying: " + new File(currentDownload.getFileLocation()).getParent() +
                                ", " + e1.getMessage(), e1);
                } finally {
                    if (showErrorMessage) {
                        logger.error("Did not successfully wait for retry download timeout: " + new File(currentDownload.getFileLocation()).getParent());
                    }
                }
            }
        };
    }
}
