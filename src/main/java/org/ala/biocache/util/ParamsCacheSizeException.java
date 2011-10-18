package org.ala.biocache.util;

/**
 *
 * @author Adam
 */
public class ParamsCacheSizeException extends Exception {
     public ParamsCacheSizeException(long sizeInBytes) {
         super("Too large for cache: " + sizeInBytes + " bytes");
     }
}
