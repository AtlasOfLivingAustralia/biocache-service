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

    <link rel='stylesheet' id='bootstrap.css-css'  href='https://www.ala.org.au/wp-content/themes/devdmbootstrap3/css/bootstrap.css?ver=1' type='text/css' media='all' />
    <link rel='stylesheet' id='parent-style-css'  href='https://www.ala.org.au/wp-content/themes/devdmbootstrap3/style.css?ver=1.5.0' type='text/css' media='all' />
    <link rel='stylesheet' id='autocompcss-css'  href='https://www.ala.org.au/wp-content/themes/ala-wordpress-theme/css/jquery.autocomplete.css?ver=1.0' type='text/css' media='all' />
    <link rel='stylesheet' id='ala-style-css'  href='https://www.ala.org.au/wp-content/themes/ala-wordpress-theme/css/ala-styles.css?ver=1.4' type='text/css' media='all' />
    <link rel='stylesheet' id='fontawesome-css'  href='//maxcdn.bootstrapcdn.com/font-awesome/4.3.0/css/font-awesome.min.css?ver=4.3.0' type='text/css' media='all' />
    <link rel='stylesheet' id='stylesheet-css'  href='https://www.ala.org.au/wp-content/themes/ala-wordpress-theme/style.css?ver=1' type='text/css' media='all' />
    <script type='text/javascript' src='https://www.ala.org.au/wp-includes/js/jquery/jquery.js?ver=1.11.2'></script>
    <script type='text/javascript' src='https://www.ala.org.au/wp-includes/js/jquery/jquery-migrate.min.js?ver=1.2.1'></script>
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
<nav id="alatopnav" class="navbar navbar-inverse navbar-fixed-top">
    <div class="container-fluid">
        <!-- Brand and toggle get grouped for better mobile display -->
        <div class="navbar-header">
            <a class="navbar-brand" href="/">
                <img alt="Brand" class="img-responsive" src="https://www.ala.org.au/wp-content/themes/ala-wordpress-theme/img/supporting-graphic-element-flat.png">
            </a>
            <button type="button" class="navbar-toggle collapsed" data-toggle="collapse" data-target="#bs-example-navbar-collapse-1">
                <span class="sr-only">Toggle navigation</span>
                <span class="icon-bar"></span>
                <span class="icon-bar"></span>
                <span class="icon-bar"></span>
            </button>
            <a class="navbar-brand font-xsmall" href="/">The Atlas Of Living Australia</a>
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
                        <li><a href="http://spatial.ala.org.au/">Spatial portal</a></li>
                        <li ><a href="http://biocache.ala.org.au/">Occurrence search</a></li>
                        <li ><a href="http://fish.ala.org.au/">Fish map</a></li>
                        <li ><a href="http://regions.ala.org.au/">Regions</a></li>
                        <li ><a href="http://biocache.ala.org.au/explore/your-area">Explore your area</a></li>

                        <li class="divider"></li>
                        <li><a href="http://sightings.ala.org.au/">Record a sighting</a></li>
                        <li><a href="http://collections.ala.org.au/">Collections</a></li>
                        <li><a href="http://volunteer.ala.org.au">DigiVol</a></li>
                        <li><a href="https://fieldcapture.ala.org.au/merit">MERIT</a></li>
                        <li><a href="http://www.soils2satellites.org.au/">Soils to satellite</a></li>
                        <li><a href="http://lists.ala.org.au/">Traits, species lists</a></li>
                        <li><a href="http://phylolink.ala.org.au/">Phylolink</a></li>

                        <li class="divider"></li>
                        <li><a href="http://root.ala.org.au/">Community portals</a></li>
                        <li><a href="http://dashboard.ala.org.au">Dashboard</a></li>
                        <li><a href="http://collections.ala.org.au/datasets">Datasets browser</a></li>
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
                            <li><a href="https://www.ala.org.auwp-login.php?redirect_to=http%3A%2F%2Fwww.ala.org.au">Log in</a></li>
                            <li><a href="https://auth.ala.org.au/userdetails/registration/createAccount">Register</a></li>
                        </ul>
                    </li>
                </ul>
            </small>

        </div>
        <!-- /.navbar-collapse --> </div>
    <!-- /.container-fluid --> </nav>

<div id="main" class="container dmbs-container">
<decorator:body/>
</div><!-- End container -->
</body>
</html>