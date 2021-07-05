/**************************************************************************
 *  Copyright (C) 2013 Atlas of Living Australia
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

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.io.IOUtils;

import java.io.*;


/**
 * A set of File Utils that may be common to ALA.
 * 
 * Some Zip utils obtained from : http://developer-tips.hubpages.com/hub/Zipping-and-Unzipping-Nested-Directories-in-Java-using-Apache-Commons-Compress
 * 
 * @author Natasha Carter (natasha.carter@csiro.au)
 *
 */
public class AlaFileUtils {

	/**
     * Creates a zip file at the specified path with the contents of the specified directory.
     * NB:
     *
     * @param directoryPath The path of the directory of files to put into the archive eg. c:/temp
     * @param zipPath The full path of the archive to create. eg. c:/output/archive.zip
     * @throws IOException If anything goes wrong
     */
    public static void createZip(String directoryPath, String zipPath) throws IOException {
        try(FileOutputStream fOut = new FileOutputStream(zipPath);
            BufferedOutputStream bOut = new BufferedOutputStream(fOut);
            ZipArchiveOutputStream tOut = new ZipArchiveOutputStream(bOut);) {

            File[] children = new File(directoryPath).listFiles();

            if (children != null) {
                for (File child : children) {
                    addFileToZip(tOut, child.getPath(), "");
                }
            }

            tOut.finish();
        }
    }

    /**
     * unpack a zip file at the specified path.
     * NB:
     *
     * @param outputDirectoryPath The path of the directory where the archive will be unpacked. eg. c:/temp
     * @param srcZipPath The full path of the zip to unpack. eg. c:/temp/archive.zip
     * @throws IOException If anything goes wrong
     */
    public static void unzip(String outputDirectoryPath, String srcZipPath) throws IOException {
        try(FileInputStream fis = new FileInputStream(srcZipPath);
        	BufferedInputStream bis = new BufferedInputStream(fis);
        	ZipArchiveInputStream zis = new ZipArchiveInputStream(bis);) {

            ZipArchiveEntry ze;
            while((ze = zis.getNextZipEntry()) != null) {
                try(FileOutputStream fos = new FileOutputStream(outputDirectoryPath + File.separator + ze.getName());
                	BufferedOutputStream bos = new BufferedOutputStream(fos);) {
                    IOUtils.copy(zis, bos);
                }
            }
        }
    }

    /**
     * Creates a zip entry for the path specified with a name built from the base passed in and the file/directory
     * name. If the path is a directory, a recursive call is made such that the full directory is added to the zip.
     *
     * @param zOut The zip file's output stream
     * @param path The filesystem path of the file/directory being added
     * @param base The base prefix to for the name of the zip file entry
     *
     * @throws IOException If anything goes wrong
     */
    private static void addFileToZip(ZipArchiveOutputStream zOut, String path, String base) throws IOException {
        File f = new File(path);
        String entryName = base + f.getName();
        ZipArchiveEntry zipEntry = new ZipArchiveEntry(f, entryName);

        zOut.putArchiveEntry(zipEntry);
 
        if (f.isFile()) {
            try(FileInputStream fInputStream = new FileInputStream(f);) {
                IOUtils.copy(fInputStream, zOut);
                zOut.closeArchiveEntry();
            }
 
        } else {
            zOut.closeArchiveEntry();
            File[] children = f.listFiles();
 
            if (children != null) {
                for (File child : children) {
                    addFileToZip(zOut, child.getAbsolutePath(), entryName + "/");
                }
            }
        }
    }
}