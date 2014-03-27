/**************************************************************************
 *  Copyright (C) 2010 Atlas of Living Australia
 *  All Rights Reserved.
 * 
 *  The contents of this file are subject to the Mozilla Public
 *  License Version 1.1 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://www.mozilla.org/MPL/
 * 
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 ***************************************************************************/
package au.org.ala.biocache.util;
import org.apache.commons.io.FilenameUtils;
import java.util.EnumSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Enum class to store and retrieve mime-types
 *
 * @author "Nick dos Remedios <Nick.dosRemedios@csiro.au>"
 */
public enum MimeType {
    HTML("text/html", "html"),
    XML("text/xml", "xml"),
    TEXT("text/plain", "txt"),
    RDF("application/rdf+xml", "xml"),
    JSON("application/json", "json"),
    PDF("application/pdf", "pdf"),
    JPEG("image/jpeg", "jpg"),
    PJPEG("image/pjpeg", "jpg"),
    CITRIX_PJPEG("image/x-citrix-pjpeg", "jpg"),
    GIF("image/gif", "gif"),
    TIF("image/tiff", "tif"),
    BMP("image/bmp", "bmp"),
    PNG("image/png", "png"),
    MP3("audio/mpeg", "mp3"),
    OGG("audio/ogg", "ogg"),
    WAV("audio/wav", "wav");

    private String mimeType;
    private String[] fileExtensions;
    // TODO: might be a requirement to support multiple extensions for a given mimetype,
    // e.g. .jpg and .jpeg. Therefore fileType will need to be a List<String> (or String[]) and client
    // code will need to be changed to accept a list instead of (scalar) String.
    //DM - This enum if primarily used to map mime types from HTTP headers to extensions for
    //files stored on the repository file system. Hence support for multiple extensions isnt necessarily required.
    // NdR - if we do harvest (say) a file called foo.jpeg, then we could alternatively map it to another
    // enum constant. E.g. JPEG2 ("image/jpeg", ".jpeg")

    /**
     * Allow reverse-lookup
     * (based on <a href="http://www.ajaxonomy.com/2007/java/making-the-most-of-java-50-enum-tricks">Enum Tricks</a>)
     */
    private static final Map<String, MimeType> mimeTypeLookup = new HashMap<String, MimeType>();
    private static final Map<String, MimeType> fileExtensionLookup = new HashMap<String, MimeType>();

    static {
        for (MimeType mt : EnumSet.allOf(MimeType.class)) {
            mimeTypeLookup.put(mt.getMimeType(), mt);
            for(String fileExtension: mt.getFileExtensions()){
                fileExtensionLookup.put(fileExtension, mt);
            }
        }
    }

    /**
     * Constructor to set mimeType
     *
     * @param mimeType
     */
    private MimeType(String mimeType, String fileExtension) {
        this.mimeType = mimeType;
        this.fileExtensions = new String[]{fileExtension};
    }

    private MimeType(String mimeType, String ... fileExtensions) {
        this.mimeType = mimeType;
        this.fileExtensions = fileExtensions;
    }

    /**
     * Get file extension, returning an empty string if unknown
     * for supplied mime type.
     *
     * @param mimetype
     * @return
     */
    public static String getFileExtension(String mimetype) {
        MimeType mimeType = MimeType.getForMimeType(mimetype);
        if (mimeType != null) {
            String[] fileExtensions = mimeType.getFileExtensions();
            if(fileExtensions.length>0)
                return fileExtensions[0];
        }
        return "";
    }

    /**
     * Lookup method for mimetype string
     *
     * @param mimetype
     * @return ContentModelEnum the ContentModelEnum
     */
    public static MimeType getForMimeType(String mimetype) {
        return mimeTypeLookup.get(mimetype);
    }

    /**
     * Lookup method for mimetype string
     *
     * @return ContentModelEnum the ContentModelEnum
     */
    public static MimeType getForFileExtension(String fileExtension) {
        return fileExtensionLookup.get(FilenameUtils.getExtension(fileExtension).toLowerCase());
    }

    /**
     * Retrieve a list of all mime types.
     *
     * @return
     */
    public static List<String> getAllMimeTypes() {
        List<String> allMimeTypes = new ArrayList<String>();
        for (MimeType mt : EnumSet.allOf(MimeType.class)) {
            allMimeTypes.add(mt.getMimeType());
        }
        return allMimeTypes;
    }

    /**
     * Retrieve a list of image mime types supported.
     *
     * @return
     */
    public static List<String> getImageMimeTypes() {
        List<String> allMimeTypes = new ArrayList<String>();
        for (MimeType mt : EnumSet.allOf(MimeType.class)) {
            //FIXME this is a bit of hack. Some sources are serving images
            //with application/octlet mime type
            if (mt.getMimeType().startsWith("image")) {
                allMimeTypes.add(mt.getMimeType());
            }
        }
        return allMimeTypes;
    }

    /**
     * @return the mime type string e.g. "image/jpeg"
     */
    public String getMimeType() {
        return mimeType;
    }

    /**
     * @see java.lang.Enum#toString()
     */
    @Override
    public String toString() {
        return mimeType;
    }

    /**
     * @return the fileExtension
     */
    public String[] getFileExtensions() {
        return fileExtensions;
    }
}