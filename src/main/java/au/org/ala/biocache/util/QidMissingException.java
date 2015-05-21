package au.org.ala.biocache.util;

/**
 *
 * @author Adam
 */
public class QidMissingException extends Exception {
     public QidMissingException(String key) {
         super("No stored query available for qid:" + key);
     }
}
