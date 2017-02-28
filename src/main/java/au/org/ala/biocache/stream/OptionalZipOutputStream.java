package au.org.ala.biocache.stream;

import au.org.ala.biocache.writer.RecordWriterError;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.ZipOutputStream;

/**
 * An OutputStream that will write ZipOutputStream entries or file name separated unzipped output.
 */
public class OptionalZipOutputStream extends OutputStream {

    public enum Type {
        zipped, unzipped
    }

    private final static String UNZIPPED_ENTRY_SEPARATOR = "------------------------------------------------------\n";

    private Type type;
    private OutputStream out;
    private ZipOutputStream zop;
    private String currentEntry;
    private long currentEntryLength;
    private int splitCount;
    private Integer maxMB;

    /**
     * Determine when a file has reached the maxMB.
     *
     * Keeps track of the length of written records so a flush is not required.
     *
     * @param writer
     * @param length number of characters written
     * @return
     * @throws IOException
     */
    public boolean isNewFile(Object writer, long length) throws IOException {
        boolean isNewFile = false;
        if (type == OptionalZipOutputStream.Type.zipped) {
            currentEntryLength += length;
            if (currentEntryLength >= maxMB * 1024 * 1024) {
                if (writer instanceof RecordWriterError) ((RecordWriterError) writer).flush();

                closeEntry();
                currentEntryLength = 0;

                splitCount++;
                String[] parts = currentEntry.split("\\.(?=[^\\.]+$)");
                zop.putNextEntry(new java.util.zip.ZipEntry(parts[0] + "_part" + splitCount + "." + parts[1]));
                isNewFile = true;
            }
        }
        return isNewFile;
    }

    public OptionalZipOutputStream(Type type, OutputStream out, Integer maxZipFileMB) {
        this.type = type;
        this.out = out;
        this.maxMB = maxZipFileMB;

        if (type == Type.zipped) {
            zop = new ZipOutputStream(out);
        }
    }

    public void putNextEntry(String name) throws IOException {
        currentEntry = name;
        currentEntryLength = 0;
        splitCount = 1;

        if (type == Type.zipped) {
            zop.putNextEntry(new java.util.zip.ZipEntry(name));
        } else {
            out.write(UNZIPPED_ENTRY_SEPARATOR.getBytes());
            out.write((name + "\n").getBytes());
        }
    }

    public void closeEntry() throws IOException {
        if (type == Type.zipped) {
            zop.closeEntry();
        } else {
            out.write("\n".getBytes());
        }
    }

    @Override
    public void write(int b) throws IOException {
        if (type == Type.zipped) {
            zop.write(b);
        } else {
            out.write(b);
        }
    }

    @Override
    public void write(byte [] b) throws IOException {
        if (type == Type.zipped) {
            zop.write(b);
        } else {
            out.write(b);
        }
    }

    @Override
    public void close() throws IOException {
        if (type == Type.zipped) {
            zop.close();
        } else {
            out.close();
        }
    }

    @Override
    public void flush() throws IOException {
        if (type == Type.zipped) {
            zop.flush();
        } else {
            out.flush();
        }
    }

    public Type getType() {
        return type;
    }

    public String getCurrentEntry() {
        return currentEntry;
    }

}
