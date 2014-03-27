<%@ page import="org.apache.solr.common.SolrException"%><%@page contentType="application/json" pageEncoding="UTF-8"%>{
<%
    try {

        // The Servlet spec guarantees this attribute will be available
        Throwable exception = (Throwable) request.getAttribute("javax.servlet.error.exception");
        exception.printStackTrace();
        if (exception != null) {
            if (exception instanceof SolrException) {
                response.setContentType("application/json");
                out.write("\"message\": \"" + exception.getMessage() + "\"");
                out.write(", \"errorType\": \"Query syntax invalid\"");
                response.setStatus(400);
            } else {
                response.setContentType("application/json");
                out.write("\"message\": \"" + exception.getMessage() + "\"");
                out.write(", \"errorType\": \"Server error\"");
                response.setStatus(500);
                //response.sendError(500);
            }
        }
    } catch (Exception ex) {
        ex.printStackTrace(new java.io.PrintWriter(out));
    }
%>
}
