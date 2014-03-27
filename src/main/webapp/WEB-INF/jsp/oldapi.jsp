<%@ page contentType="text/html" pageEncoding="UTF-8" %><%@
        taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"
        %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/fmt" prefix="fmt" %>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <meta name="pageName" content="home"/>
    <title>Occurrence Web Services | Atlas of Living Australia</title>
</head>
<body>
<style type="text/css">
    .code { font-family: courier new;}
    .webserviceList { margin-left:30px; }
    .paramList { margin-left:40px; }
    .examples  { margin-left:60px; }
    .paramOptionsList { margin-left:90px; }
    .exampleResponse { margin-left:60px; }
    strong { font-weight:bold; }
</style>
<div class="section">
<h1> Web Services </h1>
<p>
    These webservices provide spatial search capabilities for occurrence records, mapping service (WMS) and parsing services.
    <br/>
    Please send any bug reports, suggestions for improvements or new services to:
    <strong>developers 'AT' ala.org.au</strong>
</p>

<div style="color:gray;">
<h3>Occurrences</h3>
<ul class="webserviceList">

    <li><strong>Occurrence search</strong> - Performs a GET search:
        <a href="${webservicesRoot}/occurrences/search?q=*:*">/occurrences/search?q=*:*</a>
        <ul class="paramList">
            <li><strong>q</strong> - the initial query. "q=*:*" will query anything, q="macropus" will do a free text search for "macropus", q=kingdom:Fungi will search for records with a kingdom of Fungi.
                <br/>
                For a listing of the fields that can be queried in a q=INDEXEDFIELD:VALUE fashion, see <a href="${webservicesRoot}/index/fields">/index/fields</a>
            </li>
            <li><strong>fq</strong> - filters to be applied to the original query. These are additional params of the form fq=INDEXEDFIELD:VALUE e.g. fq=kingdom:Fungi. <br/>
                Again, see <a href="${webservicesRoot}/index/fields">/index/fields</a> for all the fields that a queryable.
            </li>
            <li>Extra Spatial Search Parameters - These will be applied to the initial base query in <strong>q</strong>. When no q is provided *:* will be used.
                <ul class="paramList">
                    <li><strong>lat</strong> - the decimal latitude to limit records to.  Use with lon and radius to specify a "search" circle.</li>
                    <li><strong>lon</strong> - the decimal longitude to limit records to.  Use with lat and radius to specify a "search" circle.</li>
                    <li><strong>radius</strong> - the radius in which to limit records (relative to the lat, lon point).  Use with lat and lon to specify a "search" circle.</li>
                    <li><strong>wkt</strong> - the polygon area in which to limit records. For information on Well known text, see <a href="http://en.wikipedia.org/wiki/Well-known_text">this</a> </li>
                </ul>
            </li>
            <li><strong>fl</strong> - a comma separated list of fields to use in search result.  It will use a default set of values when not supplied.
                Only the "stored" fields can be included in results.  See  <a href="${webservicesRoot}/index/fields">/index/fields</a> for all the fields.
            </li>
            <li><strong>facet</strong> - supported values are "off" or "on". By default, its "on". This is worth switching off if facetting is not required, to reduce the JSON being sent</li>
            <li><strong>facets</strong> - the fields to create facets on e.g. facets=basis_of_record.</li>
            <li><strong>pageSize</strong> - number of records to return</li>
            <li><strong>startIndex</strong> - record offset, to enable paging</li>
            <li><strong>sort</strong> - the indexed field to sort by. See <a href="${webservicesRoot}/index/fields">/index/fields</a></li>
            <li><strong>dir</strong> - supports "asc" or "desc"</li>
            <li><strong>flimit</strong> - maximum number of facets to return</li>
            <li><strong>fsort</strong> - method in which to sort the facets either "count" or "index".  This value is only applicable for facets that are NOT included in <a href="${webservicesRoot}/search/grouped/facets">/search/grouped/facets</a></li>
            <li><strong>foffset</strong> - facet offset, to enable paging</li>
            <li><strong>fprefix</strong> - limits facets to values that start with the supplied value</li>
        </ul>
        Range based faceting is supported by using the <a href="http://wiki.apache.org/solr/SimpleFacetParameters#Facet_by_Range">SOLR parameters</a>. For Example (formatted for readability):<br>
        <a href="${webservicesRoot}/occurrences/search?q=AdjustedSeedQuantity_i:[* TO *]&pageSize=0&facets=uncertainty&facet.range=ViabilitySummary_d&f.ViabilitySummary_d.facet.range.start=20.0&f.ViabilitySummary_d.facet.range.end=100.0&f.ViabilitySummary_d.facet.range.gap=10&facet.range=AdjustedSeedQuantity_i&f.AdjustedSeedQuantity_i.facet.range.start=0&f.AdjustedSeedQuantity_i.facet.range.end=100000&f.AdjustedSeedQuantity_i.facet.range.gap=50000">
            <p class="examples">
                /occurrences/search?q=AdjustedSeedQuantity_i:[* TO *]<br/>
                &pageSize=0&facets=uncertainty<br/>
                &facet.range=ViabilitySummary_d<br/>
                &f.ViabilitySummary_d.facet.range.start=20.0<br/>
                &f.ViabilitySummary_d.facet.range.end=100.0<br/>
                &f.ViabilitySummary_d.facet.range.gap=10<br/>
                &facet.range=AdjustedSeedQuantity_i<br/>
                &f.AdjustedSeedQuantity_i.facet.range.start=0<br/>
                &f.AdjustedSeedQuantity_i.facet.range.end=100000<br/>
                &f.AdjustedSeedQuantity_i.facet.range.gap=50000
            </p>
        </a>
    </li>
    <li><strong><a href="#postQueryDetails" name="postQueryDetails" id="postQueryDetails">POST query details:</a></strong>
        If you find that your q or wkt are too large or cumbersome to be passing around there is the facility to POST
        a query's detail and use a qid as a reference.  This is particularly useful in working with WKT strings that may get too large to use a GET method.
        <ul class="webserviceList">
            <li><strong>Construction:</strong> /webportal/params <br>
                POST service.<br>
                Stores q and wkt parameters.<br>
                Returns a short <b>value</b> that can be used as the initial q value in other services. e.g. q=qid:<b>value</b>
            </li>
            <li><strong>Test - a GET method to test if a short value is valid: </strong> /webportal/params/<b>value</b>
                Test if a short query parameter is valid.<br>
                Returns true or false</li>
            <li><strong>Details - a GET method to return the query that is represented by the qid: </strong> /webportal/params/details<b>value</b>
                Returns a JSON representation of the query details that have been cached. </li>
        </ul>
        You should only use the short query parameter in your subsequent searches and downloads if the "Test" service returns true.
    </li>
    <li><strong>Occurrence listing:</strong>
        <a href="${webservicesRoot}/occurrences/page">/occurrences/page</a>
        - just shows the first 10 occurrences (for debug only)</li>
    <li><strong>Occurrence view:</strong> /occurrence/{uuid} e.g. <a href="${webservicesRoot}/occurrences/35b3ff3e-a9b9-4816-a3cf-8f16cf434fc7">/occurrence/35b3ff3e-a9b9-4816-a3cf-8f16cf434fc7</a></li>
    <li><strong>Occurrence comparison view:</strong> /occurrence/compare/{uuid} e.g. <a href="${webservicesRoot}/occurrence/compare/35b3ff3e-a9b9-4816-a3cf-8f16cf434fc7.json">/occurrence/compare/35b3ff3e-a9b9-4816-a3cf-8f16cf434fc7</a></li>
    <li><strong>Occurrence for taxon:</strong> /occurrences/taxon/{guid}
        e.g. <a href="${webservicesRoot}/occurrences/taxon/urn:lsid:biodiversity.org.au:afd.taxon:7790064f-4ef7-4742-8112-6b0528d5f3fb">/occurrences/taxon/urn:lsid:biodiversity.org.au:afd.taxon:7790064f-4ef7-4742-8112-6b0528d5f3fb</a></li>
    <li><strong>Occurrence for collection:</strong> /occurrences/collections/{uid}
        e.g. <a href="${webservicesRoot}/occurrences/collections/co11">/occurrences/collections/co11</a></li>
    <li><strong>Occurrence for institution:</strong> /occurrences/institutions/{uid}
        e.g. <a href="${webservicesRoot}/occurrences/institutions/in4">/occurrences/institutions/in4</a></li>
    <li><strong>Occurrence for data resource:</strong> /occurrences/dataResources/{uid}</li>
    <li><strong>Occurrence for data provider:</strong> /occurrences/dataProviders/{uid}</li>
    <li><strong>Occurrence for data hub:</strong> /occurrences/dataHubs/{uid}
        e.g. <a href="${webservicesRoot}/occurrences/dataHubs/dh1">/occurrences/collections/dh1</a></li>
    <li><a href="#downloadFromDB" name="downloadFromDB" id="downloadFromDB"><strong>Occurrence download:</strong></a> /occurrences/download - needs request param definition<br>
        The download will include all records that satisfy the q, fq and wkt parameters.  The number of records
        for a data resource may be restricted based on a collectory configured download limit.  Params:
        <ul class="paramList">
            <li><strong>q</strong> - the initial query. "q=*:*" will query anything, q="macropus" will do a free text search for "macropus", q=kingdom:Fungi will search for records with a kingdom of Fungi.
            </li>
            <li><strong>fq</strong> - filters to be applied to the original query. These are additional params of the form fq=INDEXEDFIELD:VALUE e.g. fq=kingdom:Fungi</li>
            <li><strong>wkt</strong> - filter polygon area to be applied to the original query. For information on Well known text, see <a href="http://en.wikipedia.org/wiki/Well-known_text">this</a></li>
            <li><strong>email</strong> - the email address of the user requesting the download</li>
            <li><strong>reason</strong> - the reason for the download</li>
            <li><strong>file</strong> - the name to use for the fileto download</li>
            <li><strong>fields</strong> - a CSV list of fields to include in the download (contains a list of default)</li>
            <li><strong>extra</strong> - a CSV list of fields in include in addition to the "fields"</li>
            <li><strong>reasonTypeId</strong> - a mandatory value that indicates the reason for the download. See <a href="http://logger.ala.org.au/service/logger/reasons">reasons</a> for valid id's
            <li><strong>fileType</strong> - the file format for the download. Valid values are csv and shp. If no value is supplied csv is assumed.
            <li><strong>qa</strong> - A CSV list of record issues to include in the download. See <a href="${webservicesRoot}/occurrences/search?q=*:*&facets=assertions&pageSize=0&flimit=500">assertions</a> for possible values to include.  By default it will include all applicable issues. To include no issue supply none as the value (eg &qa=none)
        </ul>
        <p>
            Example: <a href="${webservicesRoot}/occurrences/download?q=genus:Macropus">/occurrences/download?q=genus:Macropus</a>
        </p>
        <p>
            A listing of fields that come in the download is available <a href="https://docs.google.com/spreadsheet/ccc?key=0AjNtzhUIIHeNdHhtcFVSM09qZ3c3N3ItUnBBc09TbHc">here</a>.
        </p>
    </li>
    <li><a href="#downloadFromIndex" name="downloadFromIndex" id="downloadFromIndex"><strong>Occurrence download from index:</strong></a> /occurrences/index/download - has identical request params to the "Occurrence download".<br>
        An index download differs by exporting the values from the index rather than the database. Index downloads are generally faster but they are
        restricted to values that are "stored" in the index. See  <a href="${webservicesRoot}/index/fields">/index/fields</a> for information
        about the stored index. When database fields are supplied they are mapped to the appropriate index value.<br>
        Fields that are not "stored" in the index are silently ignored in the download.
    </li>
    <li><strong>Occurrence Count for Taxa:</strong> /occurrences/taxaCount<br>
        This is a POST or GET service with the following parameters:
        <ul class="paramList">
            <li><strong>separator</strong> - the separator that will appear between taxon guids</li>
            <li><strong>guids</strong> - the list of separated taxon guids</li>
        </ul>
        Returns a map of taxon guids with the corresponding occurrence count.
    </li>
    <li><strong>Occurrence wms:</strong> /occurrences/wms - requires WMS parameters along with 'q' and 'fq' populated, if available</li>
    <li><strong>Occurrences coordinates:</strong>
        <a href="${webservicesRoot}/occurrences/coordinates">/occurrences/coordinates</a>
        - Displays a list of unique lat,lon that are used by the occurrences
    </li>
    <li><strong>List of default facets for occurrence search:</strong> <a href="${webservicesRoot}/search/facets">/search/facets</a></li>
    <li><strong>The default facets for a search grouped by theme:</strong> <a href="${webservicesRoot}/search/grouped/facets">/search/grouped/facets</a></li>
    <li><strong>List of all available index fields:</strong> <a href="${webservicesRoot}/index/fields">/index/fields</a> - A field can be used in a search or facet if indexed=true.
        A field can be accessed in a search result if stored=true.
    </li>
    <li><strong>Extra details about specific fields:</strong> /index/fields?fl=comma-separated-list-of-fields  - This can be used to get a count of distinct terms a the supplied fields<br>
        Example:<br>
        <a href="${webservicesRoot}/index/fields?fl=lat_long">/index/fields?fl=lat_long</a>

    <li><a href="#downloadFacet" name="downloadFacet" id="downloadFacet"><strong>Facet based download:</strong></a> /occurrences/facets/download - requires a 'q' and optional 'fq' and one 'facets'. Optional Params:
        <ul class="paramList">
            <li><strong>count</strong> - set to true if you would like the count included</li>
            <li><strong>lookup</strong> - set to true if you would like the download include the scientific names and higher classification for the supplied guids.  Downloads that include this param will take extra time as a lookup need to be performed</li>
            <li><strong>fsort</strong> - used to sort values in the file - either 'count' or 'index'
            <li><strong>synonym</strong> - set to true if you want the lookup to include synonyms. Use with care as this will perform an additional lookup.</li>
        </ul>
        <br>This can be used to download distinct lists of species:
        <ul>
            <li><a href="${webservicesRoot}/occurrences/facets/download?q=collection_uid:co150&facets=species_guid&lookup=true">/occurrences/facets/download?q=collection_uid:co150&facets=species_guid&lookup=true</a> - downloads a list of species guids and associated scientific names for collection co150</li>
            <li><a href="${webservicesRoot}/occurrences/facets/download?q=collection_uid:co150&facets=raw_taxon_name">/occurrences/facets/download?q=collection_uid:co150&facets=raw_taxon_name</a> - downloads a list of raw scientific names for collection co150</li>
            <li><a href="${webservicesRoot}/occurrences/facets/download?q=collection_uid:co150&facets=species_guid&count=true">/occurrences/facets/download?q=collection_uid:co150&facets=species_guid&count=true</a> - downloads a list of species guids and counts for collection co150</li>
        </ul>
    </li>
    <li><strong>Spatial Occurrence search: </strong>/occurrences/spatial - supports point-radius and wkt based searches.  To search by wkt the wkt string can be supplied directly or via a gazetteer URL. Examples:
        <ul class="paramList">
            <li><a href="${webservicesRoot}/occurrences/spatial?lat=-35.27&lon=149.15&radius=10">/occurrences/spatial?lat=-35.27&lon=149.15&radius=10</a></li>
            <li><a href="${webservicesRoot}/occurrences/spatial?wkt=POLYGON((140:-37,151:-37,151:-26,140.1310:-26,140:-37))">/occurrences/spatial?wkt=POLYGON((140:-37,151:-37,151:-26,140.1310:-26,140:-37))</a></li>
            <li><a href="${webservicesRoot}/occurrences/spatial?url=http://spatial.ala.org.au/gazetteer/lga/Acton_(Australian_Capital_Territory).xml">/occurrences/spatial?url=http://spatial.ala.org.au/gazetteer/lga/Acton_(Australian_Capital_Territory).xml</a></li>
        </ul>
    </li>
    <li><strong>Static Species Density Heatmap </strong> - returns heatmap image
        <ul class="paramList">
            <li><strong>forceRefresh</strong> - will force regeneration of the image instead of using cached version. Default is false</li>
            <li><strong>forcePointsDisplay</strong> - force to use points instead of heatmap. Default is false</li>
            <li><strong>pointColour</strong> - the point colour to use in RGB e.g. &pointColour=0000ff. Default is 0000ff (blue)</li>
            <li><strong>colourByFq</strong> - colour by a facet value e.g. &colourByFq=genus:Acacia,genus:Eucalyptus will colour the dots by different values.</li>
            <li><strong>colours</strong> - how to colour the by facet values. Note <strong>colours</strong> and <strong>colourByFq</strong> must align. e.g &colours=ff0000,00ff00</li>
            <li><strong>pointHeatMapThreshold</strong> - used to sort values in the file - either 'count' or 'index'</li>
        </ul>
        <strong>Example maps:</strong>
        <ul class="examples">
            <li><a href="${webservicesRoot}/density/map?q=Acacia">/density/map?q=Acacia</a></li>
            <li><a href="${webservicesRoot}/density/map?q=Crimson+rosella">/density/map?q=Crimson+rosella</a></li>
        </ul>
    <li><strong>Static Species Density Legend: </strong>- returns associated legend image (optional param forceRefresh=true will regenerate the image)
        <strong>Example maps:</strong>
        <ul class="examples">
            <li><a href="${webservicesRoot}/density/legend?q=*:*">/density/legend?q=*:*</a></li>
        </ul>
    </li>
</ul>

<a href="#Assertions" name="Assertions" id="Assertions"><h3>Assertions</h3></a>
<ul class="webserviceList">
    <li><strong>List assertion codes:</strong>
        <a href="${webservicesRoot}/assertions/codes">/assertions/codes</a>
    </li>
    <li><strong>Add an assertion:</strong> /occurrences/{uuid}/assertions/add</li>
    <li><strong>Add an assertion using params:</strong> /occurrences/assertions/add?recordUuid={uuid}</li>
    <li><strong>Delete an assertion:</strong> /occurrences/{uuid}/assertions/{assertionUuid}/delete</li>
    <li><strong>Delete an assertion using params:</strong> /occurrences/assertions/delete?recordUuid={uuid}&assertionUuid={assertioniUuid}</li>
    <li><strong>List assertions for occurrence:</strong> /occurrences/{uuid}/assertions/</li>
    <li><strong>List assertions for occurrence with params:</strong> /occurrences/assertions?recordUuid={uuid}</li>
    <li><strong>Add a bulk list of assertions via a POST: </strong> /bulk/assertions/add?apiKey={key}&userId={id}&userDisplayName={userDisplayName}&assertions={assertions}
        <ul class="paramList">
            <li><strong>apiKey</strong> - the shared key necessary for making edits to the system</li>
            <li><strong>userId</strong> - the id of the user to apply the assertions against</li>
            <li><strong>userDisplayName</strong> - the display name for the user that suppplied the assertions</li>
            <li><strong>assertions</strong> - a json string representation of a list of assertions, each assertion must have a recordUuid, code and comment. Example:
                   <span class="code exampleResponse"><br>
                   [     
                        {"recordUuid": "d2687ef5-7ce9-472b-b7cb-8d2ace2676e8", 
                         "code": "0",
                         "comment": "a comment"},     
                         {"recordUuid": "73aadf23-1fc2-478e-ae29-3a0646ddb3a3",
                          "code": "0",
                          "comment": "My next comment"} 
                   ]
                
                </span>
            </li>
        </ul>
    </li>

</ul>

<h3>Explore services</h3>
This section describes the services that can be used to explore a region or query.  The Explore services provide the number of distinct species
and total species counts for each of the species_groups for the supplied details.
In the services listed below the following params can be supplied:
<ul class="paramList">
    <li><strong>lat</strong> - The latitude to limit the query by (must be used with lon and radius)</li>
    <li><strong>lon</strong> - The longitude to limit the query by (must be used with lat and radius)</li>
    <li><strong>radius</strong> - The radius to from lat,lon to limit the query by (must be used with lat and lon)</li>
    <li><strong>q</strong> - The query which defaults to *:* when not supplied</li>
    <li><strong>fq</strong> - The filter query</li>
</ul>
The available services:
<ul class="webserviceList">
    <li><strong>Retrieve all species groups and counts:</strong> /explore/groups</li>
    <li><strong>Retrieve the counts for a specific group:</strong> /explore/counts/group/{group} -the first count is total number of occurrence, the second is the number of distinct species </li>
    <li><strong>Download a list of species in a group: </strong> /explore/group/{group}/download </li>
    <li><strong>Retrieve list of species and counts for a group: </strong> /explore/group/{group} - supports the additional parameters pageSize, start, sort, dir - to allow paging through the results </li>
    <li><strong>Retrieve geojson cluster for group: </strong> /geojson/radius-points - supports an additional group parameter </li>
</ul>

<h3>Breakdowns</h3>
This section outlines the breakdown services that are available.  These services are available for each of the different collectory types:
<ul class="webserviceList">
    <li>collections</li>
    <li>institutions</li>
    <li>dataResources</li>
    <li>dataProviders</li>
    <li>dataHubs</li>
</ul>
In the services specified below {collectorytype} must be one of the above values.
<ul class="webserviceList">
    <li><strong>Breakdown based on limit:</strong> /breakdown/{collectorytype}/{uid}?max={max} Example:<br>
        <a href="${webservicesRoot}/breakdown/collections/co50?max=50">/breakdown/collections/co50?max=50</a>
    </li>
    <li><strong>Breakdown of a rank:</strong> /breakdown/{collectorytype}/{uid}?rank={rank} Example:<br>
        <a href="${webservicesRoot}/breakdown/dataResources/dr375?rank=class">/breakdown/dataResources/dr375?rank=class</a>
    </li>
    <li><strong>Breakdown at the supplied name and rank: </strong> /breakdown/{collectorytype}/{uid}?rank={rank}&name={name} Example:<br>
        <a href="${webservicesRoot}/breakdown/dataHubs/dh1?rank=phylum&name=Chordata">/breakdown/dataHubs/dh1?rank=phylum&name=Chordata</a>
    </li>
</ul>

<ul class="webserviceList">
    <li><strong>Generic breakdown service is available for any query: </strong> /breakdown - available params
        <ul class="paramList">
            <li><strong>q</strong> - the initial query on which to perform the breakdown</li>
            <li><strong>fq</strong> - filters to be applied to the original query</li>
            <li><strong>max</strong> - the maximum number of names to return for the breakdown.   The rank at which the breakdown will be determined based on this limit.</li>
            <li><strong>rank</strong> - the rank at which to perform the breakdown - if a name is specified in conjunction to this the breakdown will be performed at the next level further limiting the query</li>
            <li><strong>name</strong> - the scientific name to limit the breakdown to - needs a valid rank to be supplied</li>
        </ul>
    </li>
</ul>

<h3>Administration</h3>

<ul class="webserviceList">
    <li><strong>Check for Read Only Mode: </strong><a href="${webservicesRoot}/admin/isReadOnly">/admin/isReadOnly</a></li>
    <li><strong>Modify Read Only Mode: </strong>/admin/modify?ro={true OR false}</li>
    <li><strong>Reopen Index: </strong> /admin/modify?reopenIndex=true</li>
    <li><strong>View Active Downloads: </strong><a href="${webservicesRoot}/active/download/stats">/active/download/stats</a></li>
</ul>

The remaining services in the section only support POST. All services must supply apiKey as a parameter.

<ul class="webserviceList">
    <li><strong>Optimise Index: </strong>/admin/index/optimise - This service will place the biocache-service in read only
        mode until the optimise has been completed.
        <br>Example:<br>
        curl --data "apiKey=KEY" http://biocache.ala.org.au${webservicesRoot}/admin/index/optimise
    </li>
    <li><strong>Reindex Data Resource: </strong>/admin/index/reindex - reindexes occurrences
        modified after startDate for the supplied dataResource<br>Extra Mandatory Parameters: <br>
        <ul class="paramList">
            <li>dataResource - The data resource UID to reindex</li>
            <li>startDate - The earliest modification date to reindex.</li>
        </ul>
        <br>Example:</br>
        curl --data "apiKey=KEY&dataResource=dr343&startDate=2011-07-01" http://biocache.ala.org.au${webservicesRoot}/admin/index/reindex
    </li>
</ul>

<h3>Miscellaneous</h3>
<ul class="webserviceList">
    <li><strong>Retrieve i18n mappings: </strong><a href="${webservicesRoot}/facets/i18n">/facets/i18n</a> - supports requests for language
        specific properties, such as <a href="${webservicesRoot}/facets/i18n_fr.properties">/facets/i18n_fr.properties</a> (not currently provided, contact us if required)
    </li>
    <li><strong>Is Australian test:</strong> /australian/taxon/{guid} - tests to see if the supplied GUID; occurs in Australia, has an Australian LSID or is NOT Australian. Example:<br>
        <a href="${webservicesRoot}/australian/taxon/urn:lsid:biodiversity.org.au:afd.taxon:aa745ff0-c776-4d0e-851d-369ba0e6f537">/australian/taxon/urn:lsid:biodiversity.org.au:afd.taxon:aa745ff0-c776-4d0e-851d-369ba0e6f537</a>
    </li>
    <li><strong>Images: </strong>/images/taxon/{guid} - obtains a list of occurrence images for the supplied species taxon GUID. Example:<br>
        <a href="${webservicesRoot}/images/taxon/urn:lsid:biodiversity.org.au:afd.taxon:dbc44b63-9611-44a8-af58-a29caea777b6">/images/taxon/urn:lsid:biodiversity.org.au:afd.taxon:dbc44b63-9611-44a8-af58-a29caea777b6</a></li>
    <li><a href="#deletedRecords" name="deletedRecords" id="deletedRecords"><strong>Retrieve Deleted Record UUIDs:</strong></a> /occurrence/deleted?date=yyyy-MM-dd. This service will return a list of occurrence UUIDs that have been deleted since the supplied date (inclusive).</li>
</ul>

<h3>Parsing Webservices</h3>
<ul class="webserviceList">
    <li><a href="#matchTerms" id="matchTerms" name="matchTerms"><strong>Match darwin core terms</strong></a>: /parser/matchTerms - accepts a POST JSON body. Examples:
        <ul>
            <li><span class="code exampleResponse">["scientific name", "latitude"] </span>
                <br/><strong>will return</strong><br/>
                <span  class="code exampleResponse">["scientificName", "decimalLatitude"]</span>
            </li>
            <li><span class="code exampleResponse"> ["Macropus rufus", "12.2", "149.0"] </span>
                <br/><strong>will return</strong><br/>
                <span class="code exampleResponse">["species", "decimalLatitude","decimalLongitude"] </span>
            </li>
        </ul>
    </li>
    <li><a href="#areDwcTerms" id="areDwcTerms" name="areDwcTerms"><strong>Are darwin core terms? </strong></a>: /parser/areDwcTerms - accepts a POST JSON body. Examples:
        <ul>
            <li><span class="code exampleResponse">["scientific name", "latitude"]</span>
                <br/><strong>will return</strong><br/>
                <span class="code">true</span></li>
            <li><span class="code exampleResponse">["Macropus rufus", "12.2", "149.0"]</span>
                <br/><strong>will return</strong><br/>
                <span class="code">false</span></li>
        </ul>
    </li>
    <li><a href="#adhocProcessing" id="adhocProcessing" name="adhocProcessing"><strong>Ad hoc processing</strong></a>: /process/adhoc - accepts a POST JSON body. Examples:
        <ul>
            <li><span class="code exampleResponse">{ "scientificName": "Macropus rufus" }</span>
                <br/><strong>will return</strong><br/>
                    <span class="code exampleResponse">
{"assertions":[{"comment":"Missing basis of record","problemAsserted":true,"code":20001,"name":"missingBasisOfRecord","uuid":"bf9dc7a0-9918-4ae1-ac2c-2cd9729a1dc0"},{"comment":"No date information supplied","problemAsserted":true,"code":30008,"name":"missingCollectionDate","uuid":"fe473a55-89ca-4803-b8b3-c050b930d287"}],"values":[{"processed":"Macropus rufus","raw":"","name":"species"},{"processed":"urn:lsid:biodiversity.org.au:afd.taxon:065f1da4-53cd-40b8-a396-80fa5c74dedd","raw":"","name":"phylumID"},{"processed":"urn:lsid:biodiversity.org.au:afd.taxon:e9e7db31-04df-41fb-bd8d-e0b0f3c332d6","raw":"","name":"classID"},{"processed":"urn:lsid:biodiversity.org.au:afd.taxon:6d8079f1-edc9-4aab-aabd-232a32b42471","raw":"","name":"orderID"},{"processed":"Mammalia","raw":"","name":"classs"},{"processed":"urn:lsid:biodiversity.org.au:afd.taxon:72a1c39f-2435-4c28-a680-714b69ded6f9","raw":"","name":"familyID"},{"processed":"2161700","raw":"","name":"left"},{"processed":"Chordata","raw":"","name":"phylum"},{"processed":"species","raw":"","name":"taxonRank"},{"processed":"Animalia","raw":"","name":"kingdom"},{"processed":"Macropus rufus","raw":"Macropus rufus","name":"scientificName"},{"processed":"Macropus","raw":"","name":"genus"},{"processed":"7000","raw":"","name":"taxonRankID"},{"processed":"Diprotodontia","raw":"","name":"order"},{"processed":"urn:lsid:biodiversity.org.au:afd.taxon:aa745ff0-c776-4d0e-851d-369ba0e6f537","raw":"","name":"taxonConceptID"},{"processed":"[\"Animals\",\"Mammals\"]","raw":"","name":"speciesGroups"},{"processed":"Macropodidae","raw":"","name":"family"},{"processed":"urn:lsid:biodiversity.org.au:afd.taxon:aa745ff0-c776-4d0e-851d-369ba0e6f537","raw":"","name":"speciesID"},{"processed":"urn:lsid:biodiversity.org.au:afd.taxon:558a729a-789b-4b00-a685-8843dc447319","raw":"","name":"genusID"},{"processed":"2161701","raw":"","name":"right"},{"processed":"urn:lsid:biodiversity.org.au:afd.taxon:4647863b-760d-4b59-aaa1-502c8cdf8d3c","raw":"","name":"kingdomID"},{"processed":"Red Kangaroo","raw":"","name":"vernacularName"}]}
                    </span>
            </li>
        </ul>
    </li>
</ul>

<h3>Mapping Services (/mapping)</h3>
<ul class="webserviceList">
    These services will include all records that satisfy the q, fq and wkt parameters.
    <ul>
        <li><strong>q</strong> - the initial query</li>
        <li><strong>fq</strong> - filters to be applied to the original query</li>
        <li><strong>wkt</strong> - filter polygon area to be applied to the original query</li>
        <li><strong>fl</strong> - a comma separated list of fields to include (contains a list of default)</li>
        <li><strong>pageSize</strong> - download limit (may be overridden)</li>
    </ul>

    <li><strong>Short Query Parameters:</strong>
        <ul>
            <li><strong>Construction:</strong> /mapping/params <br>
                POST service.<br>
                Stores q and wkt parameters.<br>
                Returns a short <b>value</b> that can be used as the initial q value in other services for mapping. e.g. q=qid:<b>value</b>
            </li>
            <li><strong>Test: </strong> /webportal/params/<b>value</b>
                Test if a short query parameter is valid.<br>
                Returns true or false</li>
            <li><strong>Details: </strong> /webportal/params/details<b>value</b>
                Returns a JSON representation of the query details that have been cached. </li>
        </ul>
    </li>
    <li><strong>Occurrences Bounding Box:</strong> /mapping/bbox <br>
        Returns CSV of bounding box of occurrences</li>
    <li><strong>Data Providers</strong> /webportal/dataProviders </li>
    <li><strong>Species List:</strong>
        <ul>
            <li><strong>Get species list:</strong> /mapping/species</li>
            <li><strong>Get species list as CSV:</strong> /webportal/species.csv</li>
        </ul>
    </li>
    <li><strong>Occurrences:</strong>
        <ul>
            <li><strong>Get occurrences:</strong> /mapping/occurrences</li>
            <li><strong>Get occurrences as gzipped CSV:</strong> /webportal/occurrences.gz</li>
        </ul>
    </li>
</ul>

<a href="#wmsServices" name="wmsServices" id="wmsServices"><h3>WMS Services</h3></a>

<p>These services are suitable for use with a OGC client or an OGC friendly API like <a href="http://openlayers.org/">openlayers</a>.
    Examples of use are available <a href="http://spatial.ala.org.au/ws/examples/">here</a>
</p>

<ul class="webserviceList">
    <li><strong>GetCapabilities:</strong> /ogc/ows  - generates a GetCapabilities document based on a query
        that can be used in an OGC client. The GetCapabilities gives a hierarchial taxonomic listing of taxa.
        <ul class="paramList">
            <li><strong>q</strong> - SOLR query q e.g. q=genus:Macropus</li>
            <li><strong>fq</strong> - SOLR query fq e.g. fq=genus:Macropus</li>
            <li><strong>spatiallyValidOnly</strong> - only include spatially valid coordinates when deriving taxa list</li>
            <li><strong>marineSpecies</strong> - only include marine species when deriving taxa list</li>
            <li><strong>terrestrialSpecies</strong> - only include terrestrial species when deriving taxa list</li>
            <li><strong>limitToFocus</strong> - only include species that have occurred with the focus area (e.g. Australia) when deriving taxa list</li>
            <li><strong>useSpeciesGroups</strong> - use species groups and then order to species when deriving taxa list</li>
        </ul>
    </li>
    <li><strong>GetMetadata - Marine Community Profile :</strong> /ogc/getMetadata
        <ul class="paramList">
            <li><strong>q</strong> - SOLR query q e.g. q=genus:Macropus</li>
        </ul>
    </li>
    <li><strong>GetFeatureInfo :</strong> /ogc/getFeatureInfo
        <ul class="paramList">
            <li><strong>x</strong> - x coordinate within tile</li>
            <li><strong>y</strong> - y coordinate within tile</li>
            <li><strong>WIDTH</strong> - width in pixels</li>
            <li><strong>HEIGHT</strong> - height in pixels</li>
            <li><strong>BBOX</strong> - EPSG900913 or EPSG:4326 bounding box. e.g. &BBOX=12523443.0512,-2504688.2032,15028131.5936,0.3392000021413</li>
            <li><strong>SRS</strong> - only supports EPSG900913 or EPSG:4326</li>
            <li><strong>QUERY_LAYERS</strong> - layers to query comma separated. E.g. &amp;QUERY_LAYERS=genus:Macropus,genus:Acacia</li>
        </ul>
    </li>
    <li><strong>GetMap:</strong> /mapping/wms/reflect
        <ul class="paramList">
            <li><strong>BBOX</strong> - EPSG900913 bounding box. e.g. &BBOX=12523443.0512,-2504688.2032,15028131.5936,0.3392000021413</li>
            <li><strong>WIDTH</strong> - width in pixels</li>
            <li><strong>HEIGHT</strong> - height in pixels</li>
            <li><strong>CQL_FILTER</strong> - query parameter</li>
            <li><strong>ENV</strong> - additional parameters. e.g. ENV=color%3Acd3844%3Bsize%3A3%3Bopacity%3A0.8
                <ul class="paramOptionsList">
                    <li><strong>color</strong> - hex RGB values. e.g. colour:cd3844</li>
                    <li><strong>size</strong> - radius of points in pixels</li>
                    <li><strong>opacity</strong> - opacity value 0 - 1</li>
                    <li><strong>sel</strong> - fq parameter applied to CQL_FILTER.  Matching occurrences will be highlighted on the map in a Red circle</li>
                    <li><strong>uncertainty</strong> - presence of the uncertainty parameter draws uncertainty circles to a fixed maximum of 30km</li>
                    <li><strong>colormode</strong> - facet colouring type.  <br>
                        <table style="border-bottom:none;">
                            <tr><td>colourmode</td><td>description</td></tr>
                            <tr><td>-1</td><td>(default) use color value</td></tr>
                            <tr><td>grid</td><td>map as density grid.  Grid cells drawn are not restricted to within any query WKT parameters.</td></tr>
                            <tr><td>facetname</td><td>colour as categories in a facet</td></tr>
                            <tr><td>facetname,cutpoints</td><td>colour as range in a facet using the supplied
                                comma separated cutpoints.  4 to 10 values are required.  Include minimum and maximum.
                                Minimum and maximum values do not need to be accurate.
                                e.g. colormode:year,1800,1900,1950,1970,1990,2010</td></tr>
                        </table>
                    </li>
                </ul>
            </li>
            <li><strong>STYLE</strong> - e.g. STYLE=color:cd3844;size=1;opacity=0.8
        </ul>
    </li>
    <li><strong>Legend:</strong> /mapping/legend - Get a CSV legend.<br/>
        Parameters:
        <ul class="paramList">
            <li><strong>q</strong> - CQL_FILTER value</li>
            <li><strong>cm</strong> - ENV colormode value</li>
        </ul>
        Contains columns:
        <ul class="paramList">
            <li><strong>name</strong> - legend item name</li>
            <li><strong>red</strong> - 0-255</li>
            <li><strong>green</strong> - 0-255</li>
            <li><strong>blue</strong> - 0-255</li>
            <li><strong>count</strong> - number of occurrences for this legend category in the q parameter</li>
        </ul>
    </li>
    <li><strong>Publication map image</strong> /mapping/wms/image<br/>
        e.g. <a href="${webservicesRoot}/webportal/wms/image?extents=142,-45,151,-38&q=macropus&format=jpg&dpi=300&pradiusmm=1&popacity=0.8&pcolour=0000FF&widthmm=150&scale=on&baselayer=aus2">/webportal/wms/image?extents=142,-45,151,-38&q=macropus&format=jpg&dpi=300&pradiusmm=1&popacity=0.8&pcolour=0000FF&widthmm=150&scale=on&baselayer=aus2</a>
        <ul class="paramList">
            <li><strong>extents</strong> - EPSG4326 bounding box. Valid between -180 and +180 longitude.  Latitude is best between -85 and +85.  e.g. &extents=-180,-85,180,85</li>
            <li><strong>dpi</strong> - (optional) set the image DPI.  e.g. &dpi=300</li>
            <li><strong>widthmm</strong> - (optional) width in mm, integer</li>
            <li><strong>pradiusmm</strong> - (optional) species point radius in mm, integer</li>
            <li><strong>pcolour</strong> - (optional) species point colour as hexadecimal RRGGBB.</li>
            <li><strong>popacity</strong> - (optional) species point opacity 0 - 1.</li>
            <li><strong>format</strong> - (optional) output format, png or jpg</li>
            <li><strong>scale</strong> - (optional) show scale line on the image.  Not suitable for large DPI values.  e.g. &scale=on</li>
            <li><strong>baselayer</strong> - (optional) ALA: base layer to use from http://spatial.ala.org.au/geoserver  E.g. &baselayer=aus1</li>
            <li><strong>fileName</strong> - (optional) the name of the file to pass back in content headers</li>
        </ul>
    </li>
</ul>

<a href="#outlierInformation" name="outlierInformation" id="outlierInformation"><h3>Outlier information</h3></a>
<p>These are webservices for reporting the on outlier details for records.</p>
<ul class="webserviceList">
    <li>
        <a href="#outlierInfo" name="outlierInfo" id="outlierInfo"><strong>Outlier test information for species</strong></a><br/>
        e.g.
        <a href="${webservicesRoot}/outlierInfo/urn:lsid:biodiversity.org.au:afd.taxon:0c139726-2add-4abe-a714-df67b1d4b814.json">/outlierInfo/urn:lsid:biodiversity.org.au:afd.taxon:0c139726-2add-4abe-a714-df67b1d4b814.json </a>(Mountain thornbill)
    </li>
    <li>
        <a href="#outlierRecordDetails" name="outlierRecordDetails" id="outlierRecordDetails"><strong>Outlier record details</strong></a><br/>
        e.g.
        <a href="${webservicesRoot}/outlier/record/b07bbac2-22d7-4c8a-8d61-4be1ab9e0d09">/outlier/record/b07bbac2-22d7-4c8a-8d61-4be1ab9e0d09 </a>
        (A Mountain thornbill outlier)
    </li>
</ul>

<a href="#duplicateDetection" name="duplicateDetection" id="duplicateDetection"><h3>Duplicate Detection information</h3></a>
<p>This is the webservice for reporting the details of a duplicate.  It indicates the criteria that was used to determine the duplcates.</p>
<ul class="webserviceList">
    <li><strong>/duplicates/[representative record uuid]</strong><br>eg <a href="${initParams.webservicesRoot}/duplicates/3cde1570-7a38-4a58-b121-e95c35585a29">/duplicates/3cde1570-7a38-4a58-b121-e95c35585a29</a></li>
</ul>

<a href="#queryAssertions" name="queryAssertions" id="queryAssertions"><h3>Assertion Query</h3></a>
<p>A assertion query allows a group of occurrences to have the same assertion applied at once.
    The occurrences that make up the assertions are identified by a reusable query.  New occurrences that satisfy the query will have the assertion added the next time it is applied.
    The only supported query type is based on a wkt area and species name as defined below.  In the future we may support custom queries. </p>
<p>These are webservices for working with the query assertions. </p>
<ul class="webserviceList">
    <li>
        <a href="#addQueryAssertion" name="outlierInfo" id="addQueryAssertion"><strong>Add Assertion Query</strong></a><br/>
        To add a query POST a JSON body to the following URL: /assertions/query/add.<br>
        Example JSON body
			    <span class="code exampleResponse"><br>
			    {
    "id": 4,
    "apiKey":"sharedkey",
    "status": "modified", 
    "comment": "mah comment", 
    "classification": "breeding", 
    "lastModified": "2012-07-23T16:34:34", 
    "species": "Motacilla flava", 
    "user": {
        "email": "test@test.com", 
        "authority": 1234
    }, 
    "area": "MULTIPOLYGON(((20 10,19.8078528040323 8.04909677983872,19.2387953251129 6.17316567634911,18.3146961230255 4.44429766980398,17.0710678118655 2.92893218813453,15.555702330196 1.68530387697455,13.8268343236509 0.761204674887138,11.9509032201613 0.192147195967697,10 0,8.04909677983873 0.192147195967692,6.17316567634912 0.761204674887125,4.444297669804 1.68530387697453,2.92893218813454 2.92893218813451,1.68530387697456 4.44429766980396,0.761204674887143 6.17316567634908,0.192147195967701 8.04909677983869,0 9.99999999999997,0.192147195967689 11.9509032201612,0.761204674887118 13.8268343236509,1.68530387697453 15.555702330196,2.9289321881345 17.0710678118654,4.44429766980394 18.3146961230254,6.17316567634906 19.2387953251129,8.04909677983868 19.8078528040323,9.99999999999996 20,11.9509032201612 19.8078528040323,13.8268343236509 19.2387953251129,15.555702330196 18.3146961230255,17.0710678118655 17.0710678118655,18.3146961230254 15.555702330196,19.2387953251129 13.8268343236509,19.8078528040323 11.9509032201613,20 10)))" 
}
			    </span>
    </li>
    <li>
        <a href="#viewQueryAssertion" name="viewQueryAssertion" id="viewQueryAssertion"><strong>View Assertion Query details</strong></a><br/>
        This service will return the assertion information. It will NOT return the details of the query.
        <br>/assertions/query/{uuid}</a>
    </li>
    <li>
        <a href="#viewQueryAssertions" name="viewQueryAssertions" id="viewQueryAssertions"><strong>View Query Assertions details</strong></a><br/>
        This service will return the information for all the listed assertions. It will NOT return the details of the queries.
        <br>/assertions/queries/{csv list of uuid}</a>
    </li>
    <li>
        <a href="#applyQueryAssertion" name="applyQueryAssertion" id="applyQueryAssertion"><strong>Apply Query Assertion</strong></a><br/>
        This service will apply the supplied query assertion against the biocache records.
        <br>/assertions/query/{uuid}/apply</a>
    </li>
</ul>


<a href="#endemismServices" name="endemismServices" id="endemismServices"><h3>Endemism Services</h3></a>
<p>These are webservices for reporting information about species that are endemic to specific areas.  Endemism is determined by comparing
    the list of species that occur inside to those that occur outside the supplied area.   There is a "feature" where by species that occur on the
    border of the area are considered to be inside and outside the region. </p>
<ul class="webserviceList">
    <li><strong>Count of Distinct Endemic species:</strong> /explore/counts/endemic
        <br>
        Example: <a href="${webservicesRoot}/explore/counts/endemic?q=institution_code:CSIRO&wkt=POLYGON((140:-37,151:-37,151:-26,140.131:-26,140:-37))&facets=species_guid">/explore/counts/endemic?q=institution_code:CSIRO&wkt=POLYGON((140:-37,151:-37,151:-26,140.131:-26,140:-37))&facets=species_guid</a>
    </li>
    <li><strong>Endemic Species: </strong> /explore/endemic/species
        <br>
        Example: <a href="${webservicesRoot}/explore/endemic/species?q=institution_code:CSIRO&wkt=POLYGON((140:-37,151:-37,151:-26,140.131:-26,140:-37))&facets=species_guid">/explore/endemic/species?q=institution_code:CSIRO&wkt=POLYGON((140:-37,151:-37,151:-26,140.131:-26,140:-37))&facets=species_guid</a>
    </li>
    <li><strong>Endemic Species CSV: </strong> /explore/endemic/species.csv - This service will return extra information about the species that are endemic.
        <br>
        Example: <a href="${webservicesRoot}/explore/endemic/species.csv?q=institution_code:CSIRO&wkt=POLYGON((140:-37,151:-37,151:-26,140.131:-26,140:-37))">/explore/endemic/species.csv?q=institution_code:CSIRO&wkt=POLYGON((140:-37,151:-37,151:-26,140.131:-26,140:-37))</a>
    </li>
</ul>
The services above support the following params:
<ul class="paramList">
    <li><strong>q</strong> - the initial query. "q=*:*" will query anything, q="macropus" will do a free text search for "macropus", q=kingdom:Fungi will search for records with a kingdom of Fungi.
        <br/>
        For a listing of the fields that can be queried in a q=INDEXEDFIELD:VALUE fashion, see <a href="${webservicesRoot}/index/fields">/index/fields</a>
        <br>
        This will restrict the records that are considered in then region. For example this could allow people to restrict the service to endemic birds.
    </li>
    <li><strong>fq</strong> - filters to be applied to the original query. These are additional params of the form fq=INDEXEDFIELD:VALUE e.g. fq=kingdom:Fungi. <br/>
        Again, see <a href="${webservicesRoot}/index/fields">/index/fields</a> for all the fields that a queryable.
    </li>
    <li><strong>wkt</strong> - The Well Known Text Area in which to provide the endemic species. For information on Well known text, see <a href="http://en.wikipedia.org/wiki/Well-known_text">this</a></li>
    <li><strong>facets</strong> - the field on which to based the species list.  This can be taxon_concept_lsid or species_guid.</li>
</ul>
<p>
    The endemism services support the use of POSTED q and wkt values. See  <a href="${webservicesRoot}/#postQueryDetails">POST Query Details</a> for more information.
</p>
<h2>Free text search of occurrence records (will return JSON)</h2>
<div id="inpage_search">
    <form id="search-inpage" action="${webservicesRoot}/occurrences/search" method="get" name="search-form">
        <label for="search">Search</label>
        <input type="text" class="filled ac_input" id="search" name="q" placeholder="Search the Atlas" autocomplete="off">
        <span class="search-button-wrapper"><input type="submit" class="search-button" alt="Search" value="Search"></span>
    </form>
</div>


<div id="andsFundedDiv" style="clear:both;margin-top:40px;">
    <p>

        <img src="http://www.ands.org.au/partner/ands-logo-hi-res.jpg" style="width:200px;"/>
        This project is supported by the Australian National Data Service (ANDS) through the National Collaborative Research Infrastructure Strategy Program and the Education Investment Fund (EIF) Super Science Initiative.
    </p>
</div>
</div>

</body>
</html>