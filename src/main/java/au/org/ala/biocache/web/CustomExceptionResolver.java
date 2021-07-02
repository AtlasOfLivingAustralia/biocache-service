/**************************************************************************
 *  Copyright (C) 2021 Atlas of Living Australia
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

import org.apache.solr.common.SolrException;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.handler.SimpleMappingExceptionResolver;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Biocache-service Customised exception resolver.
 *
 * This exception resolver is customised to guarantee that the model can be successfully serialized to
 * a JSON.
 * @author "xuanyu huang <hua091@csiro.au>"
 */
public class CustomExceptionResolver extends SimpleMappingExceptionResolver {
    /**
     * Customised exception resolver by using our own version of determineStatusCode.
     */
    @Override
    protected ModelAndView doResolveException(
            HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) {

        // Expose ModelAndView for chosen error view.
        String viewName = determineViewName(ex, request);
        if (viewName != null) {
            // Apply HTTP status code for error views, if specified.
            // Only apply it if we're processing a top-level request.
            Integer statusCode = determineStatusCode(request, viewName, ex);
            if (statusCode != null) {
                applyStatusCodeIfPossible(request, response, statusCode);
            }
            return getModelAndView(viewName, ex, request);
        }
        else {
            return null;
        }
    }

    /**
     * Determine the HTTP status code to apply for the given error view.
     * <p> If the exception is a solr exception, it uses the exception code as HTTP status
     * code. Otherwise it uses default implementation to get the HTTP status code.
     * @param request current HTTP request
     * @param viewName the name of the error view
     * @param ex the exception that got thrown during handler execution
     * @return the HTTP status code to use
     */
    protected Integer determineStatusCode(HttpServletRequest request, String viewName, Exception ex) {
        if (ex instanceof SolrException) {
            return ((SolrException) ex).code();
        } else {
            return determineStatusCode(request, viewName);
        }
    }

    /**
     * Return a ModelAndView for the given view name and exception.
     * <p>It overwrites the default implementation by setting a 'message' and 'errorType'
     * fields into mv.
     * @param viewName the name of the error view
     * @param ex the exception that got thrown during handler execution
     * @return the ModelAndView instance
     */
    @Override
    protected ModelAndView getModelAndView(String viewName, Exception ex) {
        ModelAndView mv = new ModelAndView(viewName);

        // if user specified 'accept: application/json' in header, ContentNegotiatingViewResolver
        // will try to convert the model into json which could crash (exception can't  be mapped to a json value)
        // so we construct a proper model here
        //
        // if no 'application/json' specified, view resolver will resolve to jsp pages in which
        // request.getAttribute("javax.servlet.error.exception") is used to get the exception and show details to user
        // this function won't be affected by this custom class so jsp pages will work as before
        mv.addObject("message", ex.getMessage());
        mv.addObject("errorType", (ex instanceof SolrException) ? "Solr error" : "Server error");
        return mv;
    }
}
