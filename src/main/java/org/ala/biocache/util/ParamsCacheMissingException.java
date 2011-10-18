package org.ala.biocache.util;

/**
 *
 * @author Adam
 */
public class ParamsCacheMissingException extends Exception {
     public ParamsCacheMissingException(long key) {
         super("No stored query available for qid:" + key);
     }
}
