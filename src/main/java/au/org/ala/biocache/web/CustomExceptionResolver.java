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

import org.apache.solr.common.SolrException;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.handler.SimpleMappingExceptionResolver;

public class CustomExceptionResolver extends SimpleMappingExceptionResolver {
    @Override
    protected ModelAndView getModelAndView(String viewName, Exception ex) {
        ModelAndView mv = new ModelAndView(viewName);

        if (ex instanceof org.springframework.dao.DataAccessException ||
            ex instanceof org.springframework.transaction.TransactionException) {
            mv.addObject("error", ex);
        } else {
            // if user specified 'accept: application/json' in header, ContentNegotiatingViewResolver
            // will try to convert the modal into json which could crash (exception can't  be mapped to a json value)
            // so we construct a proper modal here
            //
            // if no 'application/json' specified, view resolver will resolve to jsp pages in which
            // request.getAttribute("javax.servlet.error.exception") is used to get the exception and show details to user
            // this function won't be affected by this custom class so jsp pages will work as before
            mv.addObject("message", ex.getMessage());
            mv.addObject("errorType", (ex instanceof SolrException) ? "Query syntax invalid" : "Server error");
        }
        return mv;
    }
}
