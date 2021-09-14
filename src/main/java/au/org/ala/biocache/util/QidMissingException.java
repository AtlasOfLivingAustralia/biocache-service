package au.org.ala.biocache.util;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 *
 * @author Adam
 */
public class QidMissingException extends Exception {
     public QidMissingException(String key) {
         super("No stored query available for qid:" + key);
     }
}
