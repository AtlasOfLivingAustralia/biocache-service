<%@ page session="false" import="org.apache.solr.common.SolrException"%><%@ page import="au.org.ala.biocache.util.QidMissingException"%><%@ page contentType="application/json" pageEncoding="UTF-8"%>{
<%
    try {
        // The Servlet spec guarantees this attribute will be available
        Throwable exception = (Throwable) request.getAttribute("javax.servlet.error.exception");
        exception.printStackTrace();
        if (exception != null) {

            String message = exception.getMessage();
            if (message != null){
                message = message
                .replace("\"", "\\\"")
                .replaceAll("\n", "")
                .replaceAll("\r", "");;
            }

            if (exception instanceof SolrException) {
                response.setContentType("application/json");
                out.write("\"message\": \"" + message + "\"");
                out.write(", \"errorType\": \"Query syntax invalid\"");
                out.write(", \"statusCode\": 400");
                response.setStatus(400);
            } else if (exception instanceof QidMissingException) {
                response.setContentType("application/json");
                out.write("\"message\": \"" + message + "\"");
                out.write(", \"errorType\": \"Unrecognised QID\"");
                out.write(", \"statusCode\": 400");
                response.setStatus(400);
            } else {
                response.setContentType("application/json");
                out.write("\"message\": \"" + message + "\"");
                out.write(", \"errorType\": \"Server error\"");
                out.write(", \"statusCode\": 500");
                response.setStatus(500);
            }
        }
    } catch (Exception ex) {
        ex.printStackTrace(new java.io.PrintWriter(out));
    }
%>
}
