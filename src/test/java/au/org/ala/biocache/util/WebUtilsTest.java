/**************************************************************************
 *  Copyright (C) 2019 Atlas of Living Australia
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

import static org.junit.jupiter.api.Assertions.*;

import java.io.StringReader;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import au.org.ala.biocache.web.WebUtils;

/**
 * @author Peter Ansell
 */
class WebUtilsTest {

	private WebUtils testObject;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeEach
	void setUp() throws Exception {
		testObject = new WebUtils();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterEach
	void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link au.org.ala.biocache.web.WebUtils#getJson(java.lang.String)}.
	 */
	@Test
	@Disabled("Not yet implemented")
	final void testGetJsonString() throws Exception {
	}

	/**
	 * Test method for {@link au.org.ala.biocache.web.WebUtils#getJson(java.io.InputStream)}.
	 */
	@Test
	@Disabled("Not yet implemented")
	final void testGetJsonInputStream() throws Exception {
	}

	/**
	 * Test method for {@link au.org.ala.biocache.web.WebUtils#getJson(java.io.InputStream, java.nio.charset.Charset)}.
	 */
	@Test
	@Disabled("Not yet implemented")
	final void testGetJsonInputStreamCharset() throws Exception {
	}

	/**
	 * Test method for {@link au.org.ala.biocache.web.WebUtils#getJson(java.io.Reader)}.
	 */
	@Test
	final void testGetJsonReaderEmpty() throws Exception {
		Map<String, Object> testResult = testObject.getJson(new StringReader("{}"));
		assertTrue(testResult.isEmpty());
	}

	/**
	 * Test method for {@link au.org.ala.biocache.web.WebUtils#getJson(java.io.Reader)}.
	 */
	@Test
	final void testGetJsonReaderSimple() throws Exception {
		Map<String, Object> testResult = testObject.getJson(new StringReader("{ \"testValue\": 42 }"));
		assertFalse(testResult.isEmpty());
		assertEquals(42, testResult.get("testValue"));
	}

	/**
	 * Test method for {@link au.org.ala.biocache.web.WebUtils#getJson(com.fasterxml.jackson.core.JsonParser)}.
	 */
	@Test
	@Disabled("Not yet implemented")
	final void testGetJsonJsonParser() throws Exception {
	}

	/**
	 * Test method for {@link au.org.ala.biocache.web.WebUtils#createHttpClientBuilder()}.
	 */
	@Test
	@Disabled("Not yet implemented")
	final void testCreateHttpClientBuilder() throws Exception {
	}

}
