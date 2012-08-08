<%--
    Document   : main.jsp (sitemesh decorator file)
    Created on : 18/09/2009, 13:57
    Author     : dos009
--%>
<%@
        taglib prefix="decorator" uri="http://www.opensymphony.com/sitemesh/decorator" %>
<%@
        include file="/common/taglibs.jsp" %>
<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
    <title>Occurrence webservices | Atlas of Living Australia</title>

    <link rel="stylesheet" href="/static/css/base.css" type="text/css" media="screen"/>

    <link rel="stylesheet" href="http://www.ala.org.au/wp-content/themes/ala2011/style2010.css" type="text/css"
          media="screen"/>
    <link rel="stylesheet" href="http://www.ala.org.au/wp-content/themes/ala2011/style2011.css" type="text/css"
          media="screen"/>
    <link rel="stylesheet" href="http://www.ala.org.au/wp-content/themes/ala2011/css/wp-styles.css" type="text/css"
          media="screen"/>
    <link rel="stylesheet" href="http://www.ala.org.au/wp-content/themes/ala2011/css/buttons.css" type="text/css"
          media="screen"/>
    <link rel="icon" type="image/x-icon" href="http://www.ala.org.au/wp-content/themes/ala2011/images/favicon.ico"/>
    <link rel="shortcut icon" type="image/x-icon"
          href="http://www.ala.org.au/wp-content/themes/ala2011/images/favicon.ico"/>
    <link rel="stylesheet" type="text/css" media="screen"
          href="http://www.ala.org.au/wp-content/themes/ala2011/css/jquery.autocomplete.css"/>
    <link rel="stylesheet" type="text/css" media="screen"
          href="http://www.ala.org.au/wp-content/themes/ala2011/css/search.css"/>
    <link rel="stylesheet" type="text/css" media="screen"
          href="http://www.ala.org.au/wp-content/themes/ala2011/css/skin.css"/>
    <link rel="stylesheet" type="text/css" media="screen"
          href="http://www.ala.org.au/wp-content/themes/ala2011/css/sf.css"/>

    <script type="text/javascript">
        contextPath = "";
    </script>
    <!--        <script type="text/javascript" src="/hubs-webapp/static/js/jquery-1.5.min.js"></script>-->
    <script type="text/javascript" src="http://ajax.googleapis.com/ajax/libs/jquery/1.7.1/jquery.min.js"></script>
    <script type="text/javascript" src="/static/js/jquery.autocomplete.js"></script>
    <script type="text/javascript" src="/static/js/fancybox/jquery.fancybox-1.3.4.pack.js"></script>
    <script type="text/javascript" src="/static/js/jquery.ba-hashchange.min.js"></script>
    <script type="text/javascript" src="/static/js/jquery.transform.js"></script>
    <script type="text/javascript" src="/static/js/jquery.grab.js"></script>
    <script type="text/javascript" src="/static/js/jquery.jplayer.js"></script>
    <script type="text/javascript" src="/static/js/mod.csstransforms.min.js"></script>
    <script type="text/javascript" src="/static/js/circle.player.js"></script>
    <script type="text/javascript" src="/static/js/bieAutocomplete.js"></script>


    <link rel="stylesheet" href="/static/css/autocomplete.css" type="text/css" media="screen"/>
    <link rel="stylesheet" href="/static/js/fancybox/jquery.fancybox-1.3.4.css" type="text/css" media="screen"/>
    <link rel="stylesheet" type="text/css" media="screen,projection" href="/static/css/ala/widget.css"/>
    <!-- CIRCLE PLAYER -->
    <link rel="stylesheet" href="/static/css/circle.skin/circle.player.css" type="text/css" media="screen"/>


    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <meta name="decorator" content="ala"/>


    <script src="http://cdn.jquerytools.org/1.2.6/all/jquery.tools.min.js"></script>
    <script type="text/javascript" src="/static/js/jquery.cookie.js"></script>
    <link rel="stylesheet" type="text/css" href="/static/css/tabs-no-images.css"/>
    <script type="text/javascript" src="/static/js/advancedSearch.js"></script>
    <script type="text/javascript">

        /************************************************************\
         * Fire chart loading
         \************************************************************/
            //google.load("visualization", "1", {packages:["corechart"]});
            //google.setOnLoadCallback(hubChartsOnLoadCallback);

        $(document).ready(function () {
            //$("#advancedSearch").show();
            //$("ul.tabs").tabs("div.panes > div");
            $(".css-tabs:first").tabs(".css-panes:first > div", { history:true });
        });

    </script>


    <script type="text/javascript" src="http://www.ala.org.au/wp-content/themes/ala2011/scripts/html5.js"></script>
    <script language="JavaScript" type="text/javascript"
            src="http://www.ala.org.au/wp-content/themes/ala2011/scripts/superfish/superfish.js"></script>
    <script language="JavaScript" type="text/javascript"
            src="http://www.ala.org.au/wp-content/themes/ala2011/scripts/jquery.autocomplete.js"></script>
    <script language="JavaScript" type="text/javascript"
            src="http://www.ala.org.au/wp-content/themes/ala2011/scripts/uservoice.js"></script>
    <style type="text/css">
            /**************************
            to highlight the correct menu item - should be in style.css
            ***************************/
        .species .nav-species a,
        .regions .nav-locations a,
        .collections .nav-collections a,
        .datasets .nav-datasets a {
            text-decoration: none;
            background: #3d464c; /* color 3 */
            outline: 0;
            z-index: 100;
        }
    </style>
    <script type="text/javascript">

        // initialise plugins

        jQuery(function () {
            jQuery('ul.sf').superfish({
                delay:500,
                autoArrows:false,
                dropShadows:false
            });

            jQuery("form#search-form-2011 input#search-2011").autocomplete('http://bie.ala.org.au/search/auto.jsonp', {
                extraParams:{limit:100},
                dataType:'jsonp',
                parse:function (data) {
                    var rows = new Array();
                    data = data.autoCompleteList;
                    for (var i = 0; i < data.length; i++) {
                        rows[i] = {
                            data:data[i],
                            value:data[i].matchedNames[0],
                            result:data[i].matchedNames[0]
                        };
                    }
                    return rows;
                },
                matchSubset:false,
                formatItem:function (row, i, n) {
                    return row.matchedNames[0];
                },
                cacheLength:10,
                minChars:3,
                scroll:false,
                max:10,
                selectFirst:false
            });
        });
    </script>
</head>
<body class="datasets">
<div id="wrapper">

    <header id="site-header">
        <div class="inner">
            <h1 title="Atlas of Living Australia"><a href="http://www.ala.org.au"
                                                     title="Atlas of Living Australia home"><img
                    src="http://www.ala.org.au/wp-content/themes/ala2011/images/logo.png" width="315" height="33"
                    alt=""/></a></h1>
            <section id="nav-search">
                <section id="header-search">
                    <form id="search-form-2011" action="http://bie.ala.org.au/search" method="get" name="search-form">
                        <label for="search">Search</label>
                        <input id="search-2011" class="filled" title="Search" type="text" name="q"
                               placeholder="Search the Atlas"/>
                        <span class="search-button-wrapper"><button id="search-button" class="search-button"
                                                                    value="Search" type="submit"><img
                                src="http://www.ala.org.au/wp-content/themes/ala2011/images/button_search-grey.png"
                                alt="Search" width="12" height="12"/></button></span></form>
                </section>
                <nav>
                    <ol>
                        <li><a href="http://www.ala.org.au" title="Atlas of Living Australia home">Home</a></li>
                        <li class="last"><a href='https://auth.ala.org.au/cas/logout?url=http://biocache.ala.org.au/'>Log
                            out</a></li>
                    </ol>
                </nav>
            </section>
        </div>
    </header>
    <div id="border">
        <div id="content">
            <c:if test="${!empty pageContext.request.remoteUser}">
                <div id="loginId"><ala:loginStatus/></div>
            </c:if>
            <decorator:body/>
        </div>
    </div>
    <!--close border-->
</div>
<!--close wrapper_border-->
<div id="footer">
    <ala:footerMenu returnUrlPath="${requestUrl}"/>
</div>
<!--close footer-->
</div><!--close wrapper-->
</body>
</html>