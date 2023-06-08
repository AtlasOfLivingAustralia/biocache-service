package au.org.ala.biocache.stream;

import au.com.bytecode.opencsv.CSVReader;
import au.org.ala.biocache.dao.SearchDAOImpl;
import au.org.ala.biocache.dto.DownloadDetailsDTO;
import au.org.ala.biocache.dto.DownloadHeaders;
import au.org.ala.biocache.dto.Kvp;
import au.org.ala.biocache.dto.OccurrenceIndex;
import au.org.ala.biocache.service.LayersService;
import au.org.ala.biocache.service.ListsService;
import au.org.ala.biocache.util.RecordWriter;
import au.org.ala.biocache.util.SearchUtils;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.io.Tuple;

import java.io.IOException;
import java.io.Reader;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static au.org.ala.biocache.dto.OccurrenceIndex.*;

public class ProcessDownload implements ProcessInterface {

    protected static final Logger logger = Logger.getLogger(ProcessDownload.class);

    final static int MAX_BATCH_SIZE = 1000;

    ConcurrentMap<String, AtomicInteger> uidStats;
    RecordWriter recordWriter;
    DownloadDetailsDTO downloadDetails;
    boolean checkLimit;
    AtomicLong resultsCount;
    long maxDownloadSize;
    List<String> miscFields;
    DownloadHeaders headers;

    ListsService listsService;
    LayersService layersService;

    boolean includeMultivalues;
    boolean includeMisc;

    // remote analysis layer intersections require batching for performance reasons
    List<String[]> batch = new ArrayList();
    double[][] points = new double[MAX_BATCH_SIZE][2];

    String[] values = new String[0];

    long startTime = 0;

    public ProcessDownload(ConcurrentMap<String, AtomicInteger> uidStats, DownloadHeaders headers,
                           RecordWriter recordWriter, DownloadDetailsDTO downloadDetails, boolean checkLimit,
                           long maxDownloadSize,
                           ListsService listsService,
                           LayersService layersService) {
        this.uidStats = uidStats;
        this.headers = headers;
        this.recordWriter = recordWriter;
        this.downloadDetails = downloadDetails;
        this.checkLimit = checkLimit;
        this.resultsCount = downloadDetails.getRecordsDownloaded();
        this.maxDownloadSize = maxDownloadSize;
        this.miscFields = headers.miscLabels;
        this.listsService = listsService;
        this.layersService = layersService;

        this.includeMultivalues = downloadDetails == null ||
                downloadDetails.getRequestParams() == null ||
                downloadDetails.getRequestParams().getIncludeMultivalues() == null ||
                !downloadDetails.getRequestParams().getIncludeMultivalues();
        this.includeMisc = downloadDetails != null && downloadDetails.getRequestParams() != null &&
                downloadDetails.getRequestParams().getIncludeMisc();
    }

    /**
     * flush() will finish writing any rows that may be held over in the batch
     *
     * @return
     */
    public boolean flush() {
        // do analysis layer intersections before writing the batch
        intersectAnalysisLayers();

        downloadDetails.updateCounts(batch.size());

        batch.forEach(row -> recordWriter.write(row));
        batch.clear();

        downloadDetails.setMiscFields(miscFields.toArray(new String[0]));

        return true;
    }

    /**
     * process() transforms a tuple from /export query() into a single row.
     *
     * @param tuple tuple to be formatted
     * @return
     */
    public boolean process(Tuple tuple) {
        if (downloadDetails.getInterrupt()) {
            // task cancelled
            return false;
        }

        boolean finished = false;

        if (tuple.get(DATA_RESOURCE_UID) != null && (!checkLimit || (checkLimit && resultsCount.intValue() < maxDownloadSize))) {

            long count = resultsCount.getAndIncrement();

            // create a column with the correct length
            // each row consists of:
            // - requested field labels, headers.labels
            // - spatial analysisIds for intersection
            // - species list information
            int numColumns = headers.labels.length +
                    headers.analysisIds.length +
                    headers.speciesListIds.length +
                    headers.qaLabels.length +
                    headers.miscLabels.size();
            if (values.length < numColumns) {
                values = new String[numColumns];
            }

            if (count % 10000 == 0) {
                if (count > 0) {
                    logger.info("Download: " + (10000 * 1000 / (System.currentTimeMillis() - startTime)) + " records/s, " + numColumns + " columns");
                }
                startTime = System.currentTimeMillis();
            }

            appendColumns(tuple, values);

            // add species list info after field label and analysisId columns
            if (headers.speciesListIds.length > 0) {
                appendSpeciesListColumns(tuple, values, headers.labels.length + headers.analysisIds.length);
            }

            // add the assertions in separate columns
            if (headers.qaLabels.length > 0) {
                appendQaColumns(tuple, values, headers.labels.length + headers.analysisIds.length + headers.speciesListIds.length);
            }

            // Append previous and new non-empty misc fields.
            if (includeMisc) {
                values = appendMiscColumns(tuple, values, headers.labels.length + headers.analysisIds.length + headers.speciesListIds.length + headers.qaLabels.length);
            }

            //increment the counters....
            SearchDAOImpl.incrementCount(uidStats, tuple.get(INSTITUTION_UID));
            SearchDAOImpl.incrementCount(uidStats, tuple.get(COLLECTION_UID));
            SearchDAOImpl.incrementCount(uidStats, tuple.get(DATA_PROVIDER_UID));
            SearchDAOImpl.incrementCount(uidStats, tuple.get(DATA_RESOURCE_UID));

            if (headers.analysisIds.length > 0) {
                // record longitude and latitude for remote analysis layer intersections
                recordCoordinates(tuple);
                batch.add(Arrays.copyOf(values, values.length));

                if (batch.size() == MAX_BATCH_SIZE) {
                    flush();
                }
            } else {
                // batching is not required where there are no analysis layers
                recordWriter.write(values);
            }
        } else {
            // reached the record limit
            finished = true;
        }

        return finished;
    }

    private void appendQaColumns(Tuple tuple, String[] values, int offset) {
        java.util.Collection<String> assertions = tuple.getStrings("assertions");

        //Handle the case where there a no assertions against a record
        if (assertions == null) {
            assertions = Collections.EMPTY_LIST;
        }

        for (int k = 0; k < headers.qaIds.length; k++) {
            values[offset + k] = Boolean.toString(assertions.contains(headers.qaIds[k]));
        }
    }

    private void appendColumns(Tuple tuple, String[] values) {
        // get all the fields requested of SOLR, excluding post-process fields.
        // post-process fields requested are headers.included[pos] where pos >= headers.labels.length
        for (int j = 0; j < headers.labels.length; j++) {
            Object obj = tuple.get(headers.included[j]);

            if (obj == null) {
                values[j] = "";
            } else if (obj instanceof Collection) {

                Stream objStream = ((Collection) obj).stream();

                if (!includeMultivalues) {
                    objStream = objStream.limit(1);
                }

                values[j] = (String) objStream.map(SearchUtils::formatValue).collect(Collectors.joining(" | "));

            } else {
                values[j] = SearchUtils.formatValue(obj);
            }
        }
    }

    private void appendSpeciesListColumns(Tuple tuple, String[] values, int offset) {
        String lftString = String.valueOf(tuple.getString("lft"));
        String rgtString = String.valueOf(tuple.getString("rgt"));
        if (StringUtils.isNumeric(lftString)) {
            long lft = Long.parseLong(lftString);
            long rgt = Long.parseLong(rgtString);
            Kvp lftrgt = new Kvp(lft, rgt);

            String drDot = ".";
            String dr = "";
            int fieldIdx = 0;
            for (int i = 0; i < headers.speciesListIds.length; i++) {
                if (headers.speciesListIds[i].startsWith(drDot)) {
                    fieldIdx++;
                } else {
                    dr = headers.speciesListIds[i].split("\\.", 2)[0];
                    drDot = dr + ".";
                    fieldIdx = 0;
                }

                values[offset + i] = listsService.getKvpValue(fieldIdx, listsService.getKvp(dr), lftrgt);
            }
        }
    }

    private void intersectAnalysisLayers() {
        String layersServiceUrl = downloadDetails.getRequestParams().getLayersServiceUrl();

        if (batch.size() > 0 && StringUtils.isNotEmpty(layersServiceUrl) && headers.analysisIds.length > 0) {
            List<String[]> intersection = new ArrayList<String[]>();
            try {
                // only do intersection where there is at least one valid coordinate
                int i = 0;
                for (i = 0; i < batch.size(); i++) {
                    if (points[i][0] != Integer.MIN_VALUE) {
                        break;
                    }
                }
                if (i < batch.size()) {
                    Reader reader = layersService.sample(headers.analysisIds, points, null);

                    CSVReader csv = new CSVReader(reader);
                    intersection = csv.readAll();
                    csv.close();

                    for (int j = 0; j < batch.size(); j++) {
                        if (j > batch.size()) {
                            //+1 offset for header row in intersection list
                            String[] sampling = intersection.get(j + 1);
                            //+2 offset for latitude,longitude columns in sampling array
                            if (sampling != null && sampling.length == headers.analysisIds.length + 2) {
                                // suitable space is already available in each batch row String[]
                                System.arraycopy(sampling, 2, batch.get(j), headers.labels.length, sampling.length - 2);
                            }
                        }
                    }
                }
            } catch (IOException e) {
                logger.error("Failed to intersect analysis layers", e);
            }
        }
    }

    private void recordCoordinates(Tuple tuple) {
        try {
            Object lon = null;
            Object lat = null;
            if ((lon = tuple.get("sensitive_decimalLongitude")) == null || (lat = tuple.get("sensitive_decimalLatitude")) == null) {
                lon = tuple.get("decimalLongitude");
                lat = tuple.get("decimalLatitude");
            }
            if (lon == null || lat == null) {
                // set as invalid longitude
                points[batch.size()][0] = Integer.MIN_VALUE;
                points[batch.size()][1] = Integer.MIN_VALUE;
            } else {
                points[batch.size()][0] = (Double) lon;
                points[batch.size()][1] = (Double) lat;
            }
        } catch (Exception e) {
            // set the coordinates of the point to something that is invalid
            points[batch.size()][0] = Integer.MIN_VALUE;
            points[batch.size()][1] = Integer.MIN_VALUE;
        }
    }

    /**
     * Appending misc columns can change the size of 'values' when new columns are added.
     *
     * @param tuple
     * @param values
     * @return
     */
    private String[] appendMiscColumns(Tuple tuple, String[] values, int offset) {
        // append miscValues for columns found
        List<String> miscValues = new ArrayList<String>(miscFields.size());  // TODO: reuse

        // maintain miscFields order using synchronized
        synchronized (miscFields) {
            // append known miscField values
            String json = SearchUtils.formatValue(tuple.get(OccurrenceIndex.MISC));
            if (StringUtils.isNotEmpty(json)) {
                try {
                    JSONObject jo = JSONObject.fromObject(json);
                    for (String f : miscFields) {
                        values[offset] = SearchUtils.formatValue(jo.get(f));
                        offset++;

                        jo.remove(f);
                    }
                    // find and append new miscFields and their values
                    for (Object entry : jo.entrySet()) {
                        String value = SearchUtils.formatValue(((Map.Entry) entry).getValue());
                        if (StringUtils.isNotEmpty(value)) {
                            miscValues.add(value);
                            miscFields.add((String) ((Map.Entry) entry).getKey());
                        }
                    }
                } catch (Exception e) {
                    // ignore malformed dynamicProperties
                }
            }
        }

        // append miscValues to values
        if (miscValues.size() > 0) {
            String[] newValues = new String[miscValues.size() + values.length];
            System.arraycopy(values, 0, newValues, 0, values.length);
            for (int i = 0; i < miscValues.size(); i++) {
                newValues[values.length + i] = miscValues.get(i);
            }
            values = newValues;
        }

        return values;
    }
}
