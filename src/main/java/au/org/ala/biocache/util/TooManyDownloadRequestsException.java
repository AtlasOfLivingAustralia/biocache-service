package au.org.ala.biocache.util;

public class TooManyDownloadRequestsException extends Exception {
    public TooManyDownloadRequestsException() {
        super("Too many download requests. Wait until other requests finish before making new requests.");
    }
}
