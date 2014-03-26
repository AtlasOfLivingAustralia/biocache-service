/*
 * Copyright (C) 2014 Atlas of Living Australia
 * All Rights Reserved.
 *
 * The contents of this file are subject to the Mozilla Public
 * License Version 1.1 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 */

package au.org.ala.names.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Set;

import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;


public class FileUtils {


    public static Set<String> streamToSet(InputStream source, Set<String> resultSet, boolean toLowerCase) throws IOException {
        LineIterator lines = getLineIterator(source, "UTF8");
        while (lines.hasNext()) {
            String line = lines.nextLine().trim();
            if (toLowerCase)
                line = line.toLowerCase();
            // ignore comments
            if (!ignore(line)) {
                resultSet.add(line);
            }
        }
        return resultSet;
    }

    private static boolean ignore(String line) {
        if (StringUtils.trimToNull(line) == null || line.startsWith("#")) {
            return true;
        }
        return false;
    }


    /**
     * @param source   the source input stream
     * @param encoding the encoding used by the input stream
     * @return
     * @throws UnsupportedEncodingException
     */
    public static LineIterator getLineIterator(InputStream source, String encoding) {
        try {
            return new LineIterator(new BufferedReader(new InputStreamReader(source, encoding)));
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("Unsupported encoding" + encoding, e);
        }
    }


}
