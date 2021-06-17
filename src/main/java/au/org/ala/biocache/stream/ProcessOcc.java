package au.org.ala.biocache.stream;

import au.org.ala.biocache.dto.OccurrenceIndex;
import au.org.ala.biocache.dto.SearchResultDTO;
import au.org.ala.biocache.dto.SpatialSearchRequestParams;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.io.Tuple;

import java.util.Map;

public class ProcessOcc implements ProcessInterface {

    private final static Logger logger = Logger.getLogger(ProcessOcc.class);

    SearchResultDTO searchResult;
    SpatialSearchRequestParams requestParams;
    boolean includeSensitive;

    public ProcessOcc(SearchResultDTO searchResult, SpatialSearchRequestParams requestParams, boolean includeSensitive) {
        this.requestParams = requestParams;
        this.includeSensitive = includeSensitive;
        this.searchResult = searchResult;
    }

    public boolean process(Tuple tuple) {
        try {
            if (tuple != null && tuple.fieldNames.size() > 0) {
                //need to set the original q to the processed value so that we remove the wkt etc that is added from paramcache object
                Class resultClass = includeSensitive ? au.org.ala.biocache.dto.SensitiveOccurrenceIndex.class : OccurrenceIndex.class;

                OccurrenceIndex oi = (OccurrenceIndex) (new ObjectMapper()).convertValue(tuple.getMap(), resultClass);
                updateImageUrls(oi);

                searchResult.getOccurrences().add(oi);
            }

            return true;
        } catch (IllegalArgumentException e) {
            logger.warn("Failed to convert tuple to OccurrenceIndex: " + e.getMessage());
            return false;
        }
    }

    public boolean flush() {
        return true;
    }

    private void updateImageUrls(OccurrenceIndex oi) {

        if (!StringUtils.isNotBlank(oi.getImage()))
            return;

        try {
//            Map<String, String> formats = mediaStoreUrl.getImageFormats(oi.getImage());
//            oi.setImageUrl(formats.get("raw"));
//            oi.setThumbnailUrl(formats.get("thumb"));
//            oi.setSmallImageUrl(formats.get("small"));
//            oi.setLargeImageUrl(formats.get("large"));
//            String[] images = oi.getImages();
//            if (images != null && images.length > 0) {
//                String[] imageUrls = new String[images.length];
//                for (int i = 0; i < images.length; i++) {
//                    try {
//                        Map<String, String> availableFormats = mediaStoreUrl.getImageFormats(images[i]);
//                        imageUrls[i] = availableFormats.get("large");
//                    } catch (Exception ex) {
//                        logger.warn("Unable to update image URL for " + images[i] + ": " + ex.getMessage());
//                    }
//                }
//                oi.setImageUrls(imageUrls);
//            }
        } catch (Exception ex) {
            logger.warn("Unable to update image URL for " + oi.getImage() + ": " + ex.getMessage());
        }
    }
}