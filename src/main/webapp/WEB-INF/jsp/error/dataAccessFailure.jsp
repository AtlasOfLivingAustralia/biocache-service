<%--
    Document   : dataAccessFailure
    Created on : Apr 7, 2010, 12:26:36 PM
    Author     : "Nick dos Remedios <Nick.dosRemedios@csiro.au>"
--%>

<%@page contentType="text/html" pageEncoding="UTF-8"%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN"
   "http://www.w3.org/TR/html4/loose.dtd">

<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <title>Internal Error</title>
        <script type="text/javascript" src="${pageContext.request.contextPath}/static/js/jquery-1.4.2.min.js"></script>
        <script type="text/javascript">
            $(document).ready(function() {
                $(".showHideLink").toggle(function() {
                        $("#stackTrace").slideDown();
                    }, function() {
                        $("#stackTrace").slideUp();
                    }
                 );
            });
        </script>
    </head>
    <body>
        <div class="section">
            <h2/>Internal Error</h2>
            <p>Data access failure has occurred. If this error persists you might like to let us
                know <a href="mailto:webmaster@ala.org.au?subject=${pageContext.request.serverName} Error">via email</a>.</p>
            <p></p>
            <p><a href="#" class="showHideLink">Show/Hide Stacktrace</a></p>
            <div id="stackTrace" style="display: none; font-family: monospace;"><%
            try {
                    // The Servlet spec guarantees this attribute will be available
                    Throwable exception = (Throwable) request.getAttribute("javax.servlet.error.exception");

                    if (exception != null) {
                            if (exception instanceof ServletException) {
                                    // It's a ServletException: we should extract the root cause
                                    ServletException sex = (ServletException) exception;
                                    Throwable rootCause = sex.getRootCause();
                                    if (rootCause == null)
                                            rootCause = sex;
                                    System.out.println("** Root cause is: "+ rootCause.getMessage());
                                    rootCause.printStackTrace(new java.io.PrintWriter(out));
                                    exception.printStackTrace(System.out);
                            }
                            else {
                                    // It's not a ServletException, so we'll just show it
                                    System.out.println("\n** Other exception: "+ exception.getMessage());
                                    exception.printStackTrace(new java.io.PrintWriter(out));
                                    exception.printStackTrace(System.out);
                            }
                    }
                    else  {
                    System.out.println("No error information available");
                    }

                    // Display cookies
                    System.out.println("\nCookies:\n");
                    Cookie[] cookies = request.getCookies();
                    if (cookies != null) {
                    for (int i = 0; i < cookies.length; i++) {
                            //System.out.println(cookies[i].getName() + "=[" + cookies[i].getValue() + "]");
                            }
                    }

            } catch (Exception ex) {
                    ex.printStackTrace(new java.io.PrintWriter(out));
            }
            %>
            </div>
            <p/>
            <br/>
        </div>
    </body>
</html>
