<%--
    Document   : pageNotFound
    Created on : Apr 7, 2010, 12:26:36 PM
    Author     : "Natasha Carter <natasha.carter@csiro.au>"
--%>
<% response.setStatus( 500 ); %>
<%@ include file="/common/taglibs.jsp" %>
<%@page contentType="text/html" pageEncoding="UTF-8"%>
<%@ taglib prefix="form" uri="http://www.springframework.org/tags/form" %>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN"
   "http://www.w3.org/TR/html4/loose.dtd">

<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <meta name="decorator" content="${skin}"/>
        <title>Validation Error</title>
    </head>
    <body>
        <div id="column-one" class="full-width">
            <div class="section">
                <h3>Unable to process your request:</h3>
                ${errorMessage}
            </div>
        </div>
    </body>
</html>
