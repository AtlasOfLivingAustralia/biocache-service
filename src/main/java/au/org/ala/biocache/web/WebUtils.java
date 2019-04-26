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
package au.org.ala.biocache.web;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.RequestAcceptEncoding;
import org.apache.http.client.protocol.ResponseContentEncoding;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Utilities for retrieving web resources.
 * 
 * @author Peter Ansell
 */
public class WebUtils {

	private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
	private static final JsonFactory JSON_FACTORY = new JsonFactory(JSON_MAPPER);

	private final CloseableHttpClient httpClient;

	public WebUtils() {
		httpClient = createHttpClientBuilder().build();
	}

	public Map<String, Object> getJson(String url) throws IOException {
		final HttpUriRequest request = new HttpGet(url);
		request.addHeader("Accept", "application/json");
		try (CloseableHttpResponse response = httpClient.execute(request);) {
			final int status = response.getStatusLine().getStatusCode();
			if (status != 200 && status != 203) {
				throw new IOException("Can't retrieve " + url + ", status code: " + status);
			}
			try (InputStream in = response.getEntity().getContent();) {
				return getJson(in);
			}
		}
	}

	public Map<String, Object> getJson(InputStream input) throws IOException {
		try (final BOMInputStream bomInputStream = new BOMInputStream(input, false, ByteOrderMark.UTF_8,
				ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_16LE, ByteOrderMark.UTF_32BE, ByteOrderMark.UTF_32LE);) {
			Charset charset = StandardCharsets.UTF_8;
			if (bomInputStream.hasBOM()) {
				try {
					charset = Charset.forName(bomInputStream.getBOMCharsetName());
				} catch (final IllegalArgumentException e) {
					charset = StandardCharsets.UTF_8;
				}
			}
			return getJson(bomInputStream, charset);
		}
	}

	public Map<String, Object> getJson(InputStream input, Charset enc) throws IOException {
		try (InputStreamReader in = new InputStreamReader(input, enc);
				BufferedReader reader = new BufferedReader(in);) {
			return getJson(reader);
		}
	}

	public Map<String, Object> getJson(Reader reader) throws IOException {
		try (final JsonParser jp = JSON_FACTORY.createParser(reader);) {
			return getJson(jp);
		}
	}

	public Map<String, Object> getJson(JsonParser jp) throws IOException {
		final JsonToken initialToken = jp.nextToken();
		if (initialToken == JsonToken.START_OBJECT) {
			return (Map<String, Object>) jp.readValueAs(Map.class);
		} else {
			throw new JsonParseException(jp, "Expected object, found " + initialToken, jp.getCurrentLocation());
		}
	}

	public static HttpClientBuilder createHttpClientBuilder() {
		return HttpClientBuilder.create().addInterceptorFirst(new RequestAcceptEncoding())
				.addInterceptorFirst(new ResponseContentEncoding())
				.setRedirectStrategy(DefaultRedirectStrategy.INSTANCE)
				.setUserAgent("biocache-service (https://github.com/AtlasOfLivingAustralia/biocache-service")
				.useSystemProperties();
	}
}
