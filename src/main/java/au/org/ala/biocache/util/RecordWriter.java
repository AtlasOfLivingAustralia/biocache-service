package au.org.ala.biocache.util;

/**
 * A trait to be implemented by java classes to write records.
 * <p>
 * Merged from biocache-store.
 */
public interface RecordWriter {
    void initialise();

    /**
     * Writes the supplied record.
     */
    void write(String[] record);

    /**
     * Returns true if this record writer has been finalised
     */
    boolean finalised();

    void finalise();
}
