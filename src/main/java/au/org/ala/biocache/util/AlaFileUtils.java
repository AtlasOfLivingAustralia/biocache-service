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

import java.io.*;
import java.util.*;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.io.IOUtils;


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
     * @param directoryPath The path of the directory where the archive will be created. eg. c:/temp
     * @param zipPath The full path of the archive to create. eg. c:/temp/archive.zip
     * @throws IOException If anything goes wrong
     */
    public static void createZip(String directoryPath, String zipPath) throws IOException {
        FileOutputStream fOut = null;
        BufferedOutputStream bOut = null;
        ZipArchiveOutputStream tOut = null;
 
        try {
            fOut = new FileOutputStream(new File(zipPath));
            bOut = new BufferedOutputStream(fOut);
            tOut = new ZipArchiveOutputStream(bOut);
            addFileToZip(tOut, directoryPath, "");
        } finally {
            tOut.finish();
            tOut.close();
            bOut.close();
            fOut.close();
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
            FileInputStream fInputStream = null;
            try {
                fInputStream = new FileInputStream(f);
                IOUtils.copy(fInputStream, zOut);
                zOut.closeArchiveEntry();
            } finally {
                IOUtils.closeQuietly(fInputStream);
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
//    
//    public static void main(String[] args){
//        System.out.println(reduceNameByVowels("zeroCoordinates", 10));
//        System.out.println(reduceNameByVowels("unknownCountry", 10));
//        System.out.println(reduceNameByVowels("resourceTaxonomicScopeMismatch", 10));
//        String[] header = new String[]{"zeroCoordinates","resourceTaxonomicScopeMismatch","unknownCountry", "unknownCountryOrState","zeroCoordinates"};
//        System.out.println(generateShapeHeader(header));        
//    }
    
    /**
     * Reduces the supplied name to the required length, by first removing non-leading vowels and then substringing.
     * 
     * @param name
     * @param requiredLength
     * @return
     */
    public static String reduceNameByVowels(String name, int requiredLength){
        if(name.length()<= requiredLength){
            return name;
        } else{
            //remove the non-leading vowels
            name = name.replaceAll("(?!^)[aeiou]", "");
            if(name.length()>requiredLength){
                name = name.substring(0, requiredLength);
            }
            return name;
        }
    }
    
    /**
     * Generates a map of feature names to original headers.
     * @param headers
     * @return
     */
    public static Map<String,String> generateShapeHeader(String[] headers){
        Map<String,String> headerMap= new LinkedHashMap<String,String>();
        int i =0;
        for(String header :headers){
            String newHeader = reduceNameByVowels(header, 10);
            if(headerMap.containsKey(newHeader)){
                newHeader = reduceNameByVowels(header, 9) +i;
            }
            headerMap.put(newHeader, header);
            i++;
        }
        return headerMap;
    }
}