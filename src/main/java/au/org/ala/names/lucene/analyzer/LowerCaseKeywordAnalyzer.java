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
package au.org.ala.names.lucene.analyzer;

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.util.Version;

/**
 * A custom KeywordAnalyzer that converts the text to lowercase before tokenizing
 * the complete string as one token
 *
 * @author Natasha
 */
public final class LowerCaseKeywordAnalyzer extends Analyzer {

    @Override
    protected TokenStreamComponents createComponents(String fieldName,
                                                     Reader reader) {

        KeywordTokenizer src = new KeywordTokenizer(reader);
        TokenStream result = new LowerCaseFilter(Version.LUCENE_34, src);

        return new TokenStreamComponents(src, result) {

            @Override
            protected void setReader(final Reader reader) throws IOException {
                super.setReader(reader);
            }
        };
    }
}
