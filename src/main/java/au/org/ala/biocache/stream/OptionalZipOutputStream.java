package au.org.ala.biocache.stream;

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

    public OptionalZipOutputStream(Type type, OutputStream out) {
        this.type = type;
        this.out = out;

        if (type == Type.zipped) {
            zop = new ZipOutputStream(out);
        }
    }

    public void putNextEntry(String name) throws IOException {
        currentEntry = name;

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
