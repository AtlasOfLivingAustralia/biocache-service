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

public class CustomExceptionResolver extends SimpleMappingExceptionResolver {
    // This function is overridden just to use our own determineStatusCode(request, viewName, ex)
    // so if it's an SolrException its error code will be used as HTTP response status code
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

    protected Integer determineStatusCode(HttpServletRequest request, String viewName, Exception ex) {
        if (ex instanceof SolrException) {
            return ((SolrException) ex).code();
        } else {
            return determineStatusCode(request, viewName);
        }
    }

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
