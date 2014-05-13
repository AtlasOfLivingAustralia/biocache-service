<%@ page import="java.io.InputStream" %>
<%@ page import="java.util.jar.Attributes" %>
<%@ page import="java.util.jar.Manifest" %>
<%@ page contentType="text/html" pageEncoding="UTF-8" %><%@
taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" 
%><%@ taglib uri="http://java.sun.com/jsp/jstl/fmt" prefix="fmt" %>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <meta name="pageName" content="home"/>
        <title>Occurrence Web Services | Atlas of Living Australia</title>
<%
        InputStream manifestStream = application.getResourceAsStream("/META-INF/MANIFEST.MF");
        try {
            if (manifestStream != null) {
                Manifest manifest = new Manifest(manifestStream);
                Attributes attr = manifest.getMainAttributes();
                request.setAttribute("revision", attr.getValue("SCM-Revision"));
                request.setAttribute("builtBy", attr.getValue("Built-By"));
                request.setAttribute("builtWith",  attr.getValue("Build-Jdk"));
            }
        } catch (Exception e){
            e.printStackTrace();
        }
%>
    </head>
    <body>
        <div class="section">
        <h1 style="margin-bottom:20px;">Web services</h1>
        <p>
            For the API reference including examples, please see:
            <a href="http://api.ala.org.au/">http://api.ala.org.au</a>
            <br/>
            Please send any bug reports, suggestions for improvements or new services to:
            <strong>support 'AT' ala.org.au</strong>
            <br/>
            <a href="${webservicesRoot}/oldapi">Old API reference</a>
        </p>
        <p style="display:none;">
            SVN revision: ${revision} <br/>
            Built by: ${builtBy} <br/>
            Built with: ${builtWith} <br/>
        </p>
        </div>
    </body>
</html>