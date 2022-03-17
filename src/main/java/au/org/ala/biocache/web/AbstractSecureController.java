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
package au.org.ala.biocache.web;

import org.ala.client.util.Constants;
import org.apache.commons.lang3.StringUtils;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.security.web.util.matcher.IpAddressMatcher;
import org.springframework.web.client.RestOperations;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Controllers that need to perform security checks should extend this class and call shouldPerformOperation.
 * 
 * NOTE: Even though this has "Abstract" in the class name for historical reasons, it is a non-abstract class.
 */
public class AbstractSecureController {

    protected Supplier<Stream<IpAddressMatcher>> excludedNetworkStream;
    protected Supplier<Stream<IpAddressMatcher>> includedNetworkStream;

    @Value("${ratelimit.window.seconds:300}")
    protected int rateLimitWindowSeconds;

    @Value("${ratelimit.count:5}")
    protected int rateLimitCount;

    @Inject
    protected CacheManager cacheManager;

    public AbstractSecureController(){}

    /**
     * networks to exclude from rate limiting.
     * If the request IP address is within any of the networks then request will be excluded from rate limiting rules.
     *
     * @param networks array of network addresses in the format x.x.x.x/m
     */
    @Value("${ratelimit.network.exclude:#{null}}")
    void setExcludedNetworks(String[] networks) {
        if (networks != null) {
            excludedNetworkStream = () -> Arrays.stream(networks)
                    .map(IpAddressMatcher::new);
        }
    }

    /**
     * networks to include in rate limiting.
     * If the request IP address is within any of the list if networks then the request will be subject to rate limiting rules.
     *
     * @param networks array of network addresses in the format x.x.x.x/m
     */
    @Value("${ratelimit.network.include:#{null}}")
    void setIncludedNetworks(String[] networks) {
        if (networks != null) {
            includedNetworkStream = () -> Arrays.stream(networks)
                    .map(IpAddressMatcher::new);
        }
    }

    /**
     * Returns the IP address for the supplied request. It will look for the existence of
     * an X-Forwarded-For Header before extracting it from the request.
     * X-Forwarded-For Header could contain multiple ip addresses, we only return the original address
     * https://serverfault.com/questions/846489/can-x-forwarded-for-contain-multiple-ips
     * @param request
     * @return IP Address of the request
     */
    protected String getIPAddress(HttpServletRequest request) {
        String ipAddress = request.getHeader("X-Forwarded-For");
        if (ipAddress == null) {
            ipAddress = request.getRemoteAddr();
        }
        String[] ips = ipAddress.split(",");
        return (ips.length > 0) ? ips[0].trim() : null;
    }

    protected String getUserAgent(HttpServletRequest request) {
        return request.getHeader(Constants.USER_AGENT_PARAM);
    }

    /**
     * Check if the request should be rate limited.
     * The request will be rate limited if there is no 'apiKey' OR 'email' request parameter
     * OR if the IP address of the request is not in the excludedNetworks OR in the includedNetworks
     *
     * @param request
     * @return if the request should be rate limited
     * @throws IOException
     */
    public boolean rateLimitRequest(HttpServletRequest request) throws IOException {

        String ipAddress = getIPAddress(request);
        boolean ratelimitIp = true;
        if (excludedNetworkStream != null) {
            ratelimitIp &= excludedNetworkStream.get().noneMatch(networkMatcher -> networkMatcher.matches(ipAddress));
        }
        if (includedNetworkStream != null) {
            ratelimitIp |= includedNetworkStream.get().anyMatch(networkMatcher -> networkMatcher.matches(ipAddress));
        }

        if (!ratelimitIp) {
            return false;
        }

        if (rateLimitWindowSeconds > 0 && rateLimitCount > 0) {

            Cache cache = cacheManager.getCache("rateLimit");
            Cache.ValueWrapper valueWrapper = cache.get(ipAddress);
            ArrayDeque<Instant> accessTimes;

            if (valueWrapper == null) {

                accessTimes = new ArrayDeque<>();

            } else {

                accessTimes = (ArrayDeque<Instant>) valueWrapper.get();

                // remove any access times that are older then the rate limit window
                Instant windowStart = Instant.now().minusSeconds(rateLimitWindowSeconds);
                for (Instant oldestAccessTime = accessTimes.getFirst();
                     oldestAccessTime != null && oldestAccessTime.isBefore(windowStart);
                     oldestAccessTime = accessTimes.getFirst()) {
                    accessTimes.removeFirst();
                }
            }

            if (accessTimes.size() < rateLimitCount) {

                // add access times keyed by IP address only for successful requests.
                accessTimes.addLast(Instant.now());
                cache.put(ipAddress, accessTimes);

                return false;
            }
        }

        return true;
    }

    /**
     * Check the validity of the supplied key, returning false if the store is in read only mode.
     *
     * @param request The request to find the apiKey parameter from
     * @param response The response to check for {@link HttpServletResponse#isCommitted()} and to send errors on if the operation should not be committed
     * @return True if the store is not in read-only mode, the API key is valid, and the response has not already been committed, and false otherwise
     * @throws Exception If the store is in read-only mode, or the API key is invalid.
     */
    public boolean shouldPerformOperation(HttpServletRequest request, HttpServletResponse response) throws Exception {

        if (request.getUserPrincipal() == null || response.isCommitted()) {
            return false;
        }

        return true;
    }
}
