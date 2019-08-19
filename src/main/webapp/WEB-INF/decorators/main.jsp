<%@ taglib prefix="decorator" uri="http://www.opensymphony.com/sitemesh/decorator" %><%@ include file="/common/taglibs.jsp" %>
<head>
    <meta http-equiv="content-type" content="text/html; charset=UTF-8" />
    <meta name="description" content="sharing biodiversity knowledge" />
    <title>Occurrence web services | Atlas of Living Australia</title>
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link rel="pingback" href="https://www.ala.org.auxmlrpc.php" />
    <title>Occurrence webservices | Atlas of Living Australia</title>
    <link rel="alternate" type="application/rss+xml" title="Atlas of Living Australia &raquo; Feed" href="https://www.ala.org.aufeed/" />
    <link rel="alternate" type="application/rss+xml" title="Atlas of Living Australia &raquo; Comments Feed" href="https://www.ala.org.aucomments/feed/" />
    <link rel="alternate" type="application/rss+xml" title="Atlas of Living Australia &raquo; Blogs &amp; news updates Comments Feed" href="https://www.ala.org.aublogs-news/feed/" />

    <link href="https://www.ala.org.au/commonui-bs3-2019/css/bootstrap.min.css" rel="stylesheet" media="screen,print"/>
    <link href="https://www.ala.org.au/commonui-bs3-2019/css/bootstrap-theme.min.css" rel="stylesheet" media="screen,print"/>
    <link rel='stylesheet' id='parent-style-css'  href='https://www.ala.org.au/wp-content/themes/devdmbootstrap3/style.css?ver=1.5.0' type='text/css' media='all' />
    <link href="https://www.ala.org.au/commonui-bs3-2019/css/ala-styles.css" rel="stylesheet" media="screen,print"/>
    <link href="https://www.ala.org.au/commonui-bs3-2019/css/autocomplete.min.css" rel="stylesheet" media="screen,print"/>
    <link href="https://www.ala.org.au/commonui-bs3-2019/css/autocomplete-extra.min.css" rel="stylesheet" media="screen,print"/>
    <link href="https://www.ala.org.au/commonui-bs3-2019/css/font-awesome.min.css" rel="stylesheet" media="screen,print"/>
    <link rel="stylesheet" href="/assets/ala-admin-asset-b49e31ab5b68287070729cdb93ef8636.css"/>

    <script type="text/javascript" src="https://www.ala.org.au/commonui-bs3-2019/js/jquery.min.js"></script>
    <script type="text/javascript" src="https://www.ala.org.au/commonui-bs3-2019/js/jquery-migration.min.js"></script>
    <script type="text/javascript" src="https://www.ala.org.au/commonui-bs3-2019/js/autocomplete.min.js"></script>
    <script type="text/javascript" src="https://www.ala.org.au/commonui-bs3-2019/js/application.js" defer></script>
    <script type="text/javascript" src="https://www.ala.org.au/commonui-bs3-2019/js/bootstrap.min.js"></script>

    <link rel="EditURI" type="application/rsd+xml" title="RSD" href="https://www.ala.org.auxmlrpc.php?rsd" />
    <link rel="wlwmanifest" type="application/wlwmanifest+xml" href="https://www.ala.org.au/wp-includes/wlwmanifest.xml" />
    <!-- END GADWP Universal Tracking -->

    <!-- Favicon -->
    <link rel="apple-touch-icon" sizes="57x57" href="https://www.ala.org.au/wp-content/themes/ala-wordpress-theme/img/favicon/apple-icon-57x57.png">
    <link rel="apple-touch-icon" sizes="60x60" href="https://www.ala.org.au/wp-content/themes/ala-wordpress-theme/img/favicon/apple-icon-60x60.png">
    <link rel="apple-touch-icon" sizes="72x72" href="https://www.ala.org.au/wp-content/themes/ala-wordpress-theme/img/favicon/apple-icon-72x72.png">
    <link rel="apple-touch-icon" sizes="76x76" href="https://www.ala.org.au/wp-content/themes/ala-wordpress-theme/img/favicon/apple-icon-76x76.png">
    <link rel="apple-touch-icon" sizes="114x114" href="https://www.ala.org.au/wp-content/themes/ala-wordpress-theme/img/favicon/apple-icon-114x114.png">
    <link rel="apple-touch-icon" sizes="120x120" href="https://www.ala.org.au/wp-content/themes/ala-wordpress-theme/img/favicon/apple-icon-120x120.png">
    <link rel="apple-touch-icon" sizes="144x144" href="https://www.ala.org.au/wp-content/themes/ala-wordpress-theme/img/favicon/apple-icon-144x144.png">
    <link rel="apple-touch-icon" sizes="152x152" href="https://www.ala.org.au/wp-content/themes/ala-wordpress-theme/img/favicon/apple-icon-152x152.png">
    <link rel="apple-touch-icon" sizes="180x180" href="https://www.ala.org.au/wp-content/themes/ala-wordpress-theme/img/favicon/apple-icon-180x180.png">
    <link rel="icon" type="image/png" sizes="192x192" href="https://www.ala.org.au/wp-content/themes/ala-wordpress-theme/img/favicon/android-icon-192x192.png">
    <link rel="icon" type="image/png" sizes="32x32" href="https://www.ala.org.au/wp-content/themes/ala-wordpress-theme/img/favicon/favicon-32x32.png">
    <link rel="icon" type="image/png" sizes="96x96" href="https://www.ala.org.au/wp-content/themes/ala-wordpress-theme/img/favicon/favicon-96x96.png">
    <link rel="icon" type="image/png" sizes="16x16" href="https://www.ala.org.au/wp-content/themes/ala-wordpress-theme/img/favicon/favicon-16x16.png">

    <!-- HTML5 shim and Respond.js for IE8 support of HTML5 elements and media queries -->
    <!-- WARNING: Respond.js doesn't work if you view the page via file:// -->
    <!--[if lt IE 9]>
    <script src="https://oss.maxcdn.com/html5shiv/3.7.2/html5shiv.min.js"></script>
    <script src="https://oss.maxcdn.com/respond/1.4.2/respond.min.js"></script>
    <![endif]-->
</head>
<body>

<!-- Navbar -->
<div id="wrapper-navbar" itemscope="" itemtype="http://schema.org/WebSite">
<nav class="navbar navbar-inverse navbar-expand-md">
    <div class="container-fluid header-logo-menu">
        <!-- Brand and toggle get grouped for better mobile display -->
        <div class="navbar-header">
            <div>
                <a href="https://www.ala.org.au/" class="custom-logo-link navbar-brand" itemprop="url">
                    <img width="1005" height="150" src="https://www.ala.org.au/app/uploads/2019/01/logo.png"
                         class="custom-logo" alt="Atlas of Living Australia" itemprop="image"
                         srcset="https://www.ala.org.au/app/uploads/2019/01/logo.png 1005w, https://www.ala.org.au/app/uploads/2019/01/logo-300x45.png 300w, https://www.ala.org.au/app/uploads/2019/01/logo-768x115.png 768w"
                         sizes="(max-width: 1005px) 100vw, 1005px"> </a>
                <!-- end custom logo -->
            </div>
        </div>

        <!-- Collect the nav links, forms, and other content for toggling -->
        <div class="collapse navbar-collapse" id="bs-example-navbar-collapse-1">
            <ul class="nav navbar-nav">
                <li >
                    <a href="/about-the-atlas/contact-us/">
                        Contact us
                        <span class="sr-only">(current)</span>
                    </a>
                </li>
                <li >
                    <a href="/get-involved/">Get involved</a>
                </li>

                <li class="dropdown font-xsmall">
                    <a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-expanded="false">
                        ALA Apps
                        <span class="caret"></span>
                    </a>
                    <ul class="dropdown-menu" role="menu">
                        <li><a href="https://spatial.ala.org.au/">Spatial portal</a></li>
                        <li ><a href="https://biocache.ala.org.au/">Occurrence search</a></li>
                        <li ><a href="https://fish.ala.org.au/">Fish map</a></li>
                        <li ><a href="https://regions.ala.org.au/">Regions</a></li>
                        <li ><a href="https://biocache.ala.org.au/explore/your-area">Explore your area</a></li>

                        <li class="divider"></li>
                        <li><a href="https://sightings.ala.org.au/">Record a sighting</a></li>
                        <li><a href="https://collections.ala.org.au/">Collections</a></li>
                        <li><a href="https://volunteer.ala.org.au">DigiVol</a></li>
                        <li><a href="https://fieldcapture.ala.org.au/merit">MERIT</a></li>
                        <li><a href="http://www.soils2satellites.org.au/">Soils to satellite</a></li>
                        <li><a href="https://lists.ala.org.au/">Traits, species lists</a></li>
                        <li><a href="https://phylolink.ala.org.au/">Phylolink</a></li>

                        <li class="divider"></li>
                        <li><a href="http://root.ala.org.au/">Community portals</a></li>
                        <li><a href="https://dashboard.ala.org.au">Dashboard</a></li>
                        <li><a href="https://collections.ala.org.au/datasets">Datasets browser</a></li>
                    </ul>
                </li>

            </ul>
            <form class="navbar-form navbar-left" role="search" action="https://bie.ala.org.au/search" method="get">
                <div class="form-group">
                    <input id="search" class="autocomplete form-control" title="Search" type="text" name="q" placeholder="Search the Atlas" autocomplete="off">
                </div>
                <button type="submit" class="btn btn-primary">Search</button>
            </form>

            <small>
                <ul class="nav navbar-nav navbar-right">
                    <li class="dropdown font-xsmall">
                        <a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-expanded="false">
                            User settings
                            <span class="caret"></span>
                        </a>
                        <ul class="dropdown-menu" role="menu">
                            <li><a href="https://auth.ala.org.au/cas/login?service=https://biocache.ala.org.au/">Log in</a></li>
                            <li><a href="https://auth.ala.org.au/userdetails/registration/createAccount">Register</a></li>
                        </ul>
                    </li>
                </ul>
            </small>

        </div>
        <!-- /.navbar-collapse --> </div>
    <!-- /.container-fluid --> </nav>
</div>
<div id="main" class="container dmbs-container">
<decorator:body/>
</div><!-- End container -->
</body>
</html>
