<%@ page contentType="text/html" pageEncoding="UTF-8" %><%@ 
taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" 
%><!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <meta name="pageName" content="home"/>
        <title>BioCache  Webservices | Atlas of Living Australia</title>
    </head>
    <body>
        <style type="text/css">
            .code { font-family: courier new;}
            .webserviceList { margin-left:30px; }
            .paramList { margin-left:60px; }
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
        
        <h3>Occurrences</h3>
        <ul class="webserviceList">

            <li><strong>Occurrence search</strong> - Performs a GET search:           
                <a href="${initParam.webservicesRoot}/occurrences/search?q=*:*">/occurrences/search?q=*:*</a>
                <ul class="paramList">
                    <li><strong>q</strong> - the initial query. "q=*:*" will query anything, q="macropus" will do a free text search for "macropus", q=kingdom:Fungi will search for records with a kingdom of Fungi.
                        <br/>
                        For a listing of the fields that can be queried in a q=INDEXEDFIELD:VALUE fashion, see <a href="${initParam.webservicesRoot}/index/fields">/index/fields</a>
                    </li>
                    <li><strong>fq</strong> - filters to be applied to the original query. These are additional params of the form fq=INDEXEDFIELD:VALUE e.g. fq=kingdom:Fungi. <br/>
                        Again, see <a href="${initParam.webservicesRoot}/index/fields">/index/fields</a> for all the fields that a queryable.
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
                    	Only the "stored" fields can be included in results.  See  <a href="${initParam.webservicesRoot}/index/fields">/index/fields</a> for all the fields. 
                    </li>
                    <li><strong>facet</strong> - supported values are "off" or "on". By default, its "on". This is worth switching off if facetting is not required, to reduce the JSON being sent</li>
                    <li><strong>facets</strong> - the fields to create facets on e.g. facets=basis_of_record.</li>
                    <li><strong>pageSize</strong> - number of records to return</li>
                    <li><strong>startIndex</strong> - record offset, to enable paging</li>
                    <li><strong>sort</strong> - the indexed field to sort by. See <a href="${initParam.webservicesRoot}/index/fields">/index/fields</a></li>
                    <li><strong>dir</strong> - supports "asc" or "desc"</li>
                    <li><strong>flimit</strong> - maximum number of facets to return</li>
                    <li><strong>fsort</strong> - method in which to sort the facets either "count" or "index".  This value is only applicable for facets that are NOT included in <a href="${initParam.webservicesRoot}/search/grouped/facets">/search/grouped/facets</a></li>
                    <li><strong>foffset</strong> - facet offset, to enable paging</li>
                    <li><strong>fprefix</strong> - limits facets to values that start with the supplied value</li>                    
                </ul>
            </li>
            <li><strong>POST query details:</strong>
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
                </ul>
                You should only use the short query parameter in your subsequent searches and downloads if the "Test" service returns true.
            </li>           
            <li><strong>Occurrence listing:</strong>
                <a href="${initParam.webservicesRoot}/occurrences/page">/occurrences/page</a>
                - just shows the first 10 occurrences (for debug only)</li>
            <li><strong>Occurrence view:</strong> /occurrence/{uuid} e.g. <a href="${initParam.webservicesRoot}/occurrences/35b3ff3e-a9b9-4816-a3cf-8f16cf434fc7">/occurrence/35b3ff3e-a9b9-4816-a3cf-8f16cf434fc7</a></li>
            <li><strong>Occurrence comparison view:</strong> /occurrence/compare/{uuid} e.g. <a href="${initParam.webservicesRoot}/occurrence/compare/35b3ff3e-a9b9-4816-a3cf-8f16cf434fc7.json">/occurrence/compare/35b3ff3e-a9b9-4816-a3cf-8f16cf434fc7</a></li>
            <li><strong>Occurrence for taxon:</strong> /occurrences/taxon/{guid}
                e.g. <a href="${initParam.webservicesRoot}/occurrences/taxon/urn:lsid:biodiversity.org.au:afd.taxon:7790064f-4ef7-4742-8112-6b0528d5f3fb">/occurrences/taxon/urn:lsid:biodiversity.org.au:afd.taxon:7790064f-4ef7-4742-8112-6b0528d5f3fb</a></li>
            <li><strong>Occurrence for collection:</strong> /occurrences/collections/{uid}
                e.g. <a href="${initParam.webservicesRoot}/occurrences/collections/co11">/occurrences/collections/co11</a></li>
            <li><strong>Occurrence for institution:</strong> /occurrences/institutions/{uid}
                e.g. <a href="${initParam.webservicesRoot}/occurrences/institutions/in4">/occurrences/institutions/in4</a></li>
            <li><strong>Occurrence for data resource:</strong> /occurrences/dataResources/{uid}</li>
            <li><strong>Occurrence for data provider:</strong> /occurrences/dataProviders/{uid}</li>
            <li><strong>Occurrence for data hub:</strong> /occurrences/dataHubs/{uid}
                e.g. <a href="${initParam.webservicesRoot}/occurrences/dataHubs/dh1">/occurrences/collections/dh1</a></li>
            <li><strong>Occurrence download:</strong> /occurrences/download - needs request param definition<br>
                The download will include all records that satisfy the q, fq and wkt parameters.  The number of records
                for a data resource may be restricted based on a collectory configured download limit.  Params:
                <ul class="paramList">
                    <li><strong>q</strong> - the initial query. "q=*:*" will query anything, q="macropus" will do a free text search for "macropus", q=kingdom:Fungi will search for records with a kingdom of Fungi.
                        <br/>

                    </li>
                    <li><strong>fq</strong> - filters to be applied to the original query. These are additional params of the form fq=INDEXEDFIELD:VALUE e.g. fq=kingdom:Fungi</li>
                    <li><strong>wkt</strong> - filter polygon area to be applied to the original query. For information on Well known text, see <a href="http://en.wikipedia.org/wiki/Well-known_text">this</a></li>
                    <li><strong>email</strong> - the email address of the user requesting the download</li>
                    <li><strong>reason</strong> - the reason for the download</li>
                    <li><strong>file</strong> - the name to use for the fileto download</li>
                    <li><strong>fields</strong> - a CSV list of fields to include in the download (contains a list of default)</li>
                    <li><strong>extra</strong> - a CSV list of fields in include in addition to the "fields"</li>
                </ul>
                <p>
                    Example: <a href="${initParam.webservicesRoot}/occurrences/download?q=genus:Macropus">/occurrences/download?q=genus:Macropus</a>
                </p>
                <p>
                    A listing of fields that come in the download is available <a href="https://docs.google.com/spreadsheet/ccc?key=0AjNtzhUIIHeNdHhtcFVSM09qZ3c3N3ItUnBBc09TbHc">here</a>.
                </p>
             </li>
             <li><strong>Occurrence download from index:</strong> /occurrences/index/download - has identical request params to the "Occurrence download".<br>
             	An index download differs by exporting the values from the index rather than the database. Index downloads are generally faster but they are 
             	restricted to values that are "stored" in the index. See  <a href="${initParam.webservicesRoot}/index/fields">/index/fields</a> for information
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
            <li><strong>Occurrence static:</strong> /occurrences/static - generates an image of AU with points on, filtering on 'q' and 'fq', if available</li>
            <li><strong>Occurrences coordinates:</strong>
                <a href="${initParam.webservicesRoot}/occurrences/coordinates">/occurrences/coordinates</a>
                - Displays a list of unique lat,lon that are used by the occurrences
                 </li>
            <li><strong>List of default facets for occurrence search:</strong> <a href="${initParam.webservicesRoot}/search/facets">/search/facets</a></li>
            <li><strong>The default facets for a search grouped by theme:</strong> <a href="${initParam.webservicesRoot}/search/grouped/facets">/search/grouped/facets</a></li>
            <li><strong>List of all available index fields:</strong> <a href="${initParam.webservicesRoot}/index/fields">/index/fields</a> - A field can be used in a search or facet if indexed=true.  A field can be accessed in a search result if stored=true.</li>
            <li><strong>Extra details about specific fields:</strong> /index/fields?fl=comma-separated-list-of-fields  - This can be used to get a count of distinct terms a the supplied fields<br>
            Example:<br>
            <a href="${initParam.webservicesRoot}/index/fields?fl=lat_long">/index/fields?fl=lat_long</a>           
            <li><strong>Facet based download:</strong> /occurrences/facets/download - requires a 'q' and optional 'fq' and one 'facets'. Optional Params:
                <ul class="paramList">
            	<li><strong>count</strong> - set to true if you would like the count included</li>
            	<li><strong>lookup</strong> - set to true if you would like the download include the scientific names for the supplied guids.  Downloads that include this param will take extra time as a lookup need to be performed</li>
            	<li><strong>fsort</strong> - used to sort values in the file - either 'count' or 'index'
            </ul> 
            <br>This can be used to download distinct lists of species:
            <ul>
            	<li><a href="${initParam.webservicesRoot}/occurrences/facets/download?q=collection_uid:co150&facets=species_guid&lookup=true">/occurrences/facets/download?q=collection_uid:co150&facets=species_guid&lookup=true</a> - downloads a list of species guids and associated scientific names for collection co150</li>
            	<li><a href="${initParam.webservicesRoot}/occurrences/facets/download?q=collection_uid:co150&facets=raw_taxon_name">/occurrences/facets/download?q=collection_uid:co150&facets=raw_taxon_name</a> - downloads a list of raw scientific names for collection co150</li>
            	<li><a href="${initParam.webservicesRoot}/occurrences/facets/download?q=collection_uid:co150&facets=species_guid&count=true">/occurrences/facets/download?q=collection_uid:co150&facets=species_guid&count=true</a> - downloads a list of species guids and counts for collection co150</li>
            </ul>
            </li>
            <li><strong>Spatial Occurrence search: </strong>/occurrences/spatial - supports point-radius and wkt based searches.  To search by wkt the wkt string can be supplied directly or via a gazetteer URL. Examples:
            	<ul>
            		<li><a href="${initParam.webservicesRoot}/occurrences/spatial?lat=-35.27&lon=149.15&radius=10">/occurrences/spatial?lat=-35.27&lon=149.15&radius=10</a></li>
            		<li><a href="${initParam.webservicesRoot}/occurrences/spatial?wkt=POLYGON((140:-37,151:-37,151:-26,140.1310:-26,140:-37))">/occurrences/spatial?wkt=POLYGON((140:-37,151:-37,151:-26,140.1310:-26,140:-37))</a></li>
            		<li><a href="${initParam.webservicesRoot}/occurrences/spatial?url=http://spatial.ala.org.au/gazetteer/lga/Acton_(Australian_Capital_Territory).xml">/occurrences/spatial?url=http://spatial.ala.org.au/gazetteer/lga/Acton_(Australian_Capital_Territory).xml</a></li>
            	</ul> 
            </li>
            <li><strong>Static Species Density Heatmap </strong><a href="${initParam.webservicesRoot}/density/map?q=*:*">/density/map?q=*:*</a></li> - returns heatmap image (optional param forceRefresh=true will regenerate the image)
            <li><strong>Static Species Density Legend: </strong><a href="${initParam.webservicesRoot}/density/legend?q=*:*">/density/legend?q=*:*</a></li> - returns associated legend image (optional param forceRefresh=true will regenerate the image)
        </ul>

        <h3>Assertions</h3>
        <ul class="webserviceList">
            <li><strong>List assertion codes:</strong>
                <a href="${initParam.webservicesRoot}/assertions/codes">/assertions/codes</a>
            </li>
            <li><strong>Add an assertion:</strong> /occurrences/{uuid}/assertions/add</li>
            <li><strong>Add an assertion using params:</strong> /occurrences/assertions/add?recordUuid={uuid}</li>
            <li><strong>Delete an assertion:</strong> /occurrences/{uuid}/assertions/{assertionUuid}/delete</li>
            <li><strong>Delete an assertion using params:</strong> /occurrences/assertions/delete?recordUuid={uuid}&assertionUuid={assertioniUuid}</li>
            <li><strong>List assertions for occurrence:</strong> /occurrences/{uuid}/assertions/</li>
            <li><strong>List assertions for occurrence with params:</strong> /occurrences/assertions?recordUuid={uuid}</li>
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
            <a href="${initParam.webservicesRoot}/breakdown/collections/co50?max=50">/breakdown/collections/co50?max=50</a> 
            </li>           
            <li><strong>Breakdown of a rank:</strong> /breakdown/{collectorytype}/{uid}?rank={rank} Example:<br>
            <a href="${initParam.webservicesRoot}/breakdown/dataResources/dr375?rank=class">/breakdown/dataResources/dr375?rank=class</a>
            </li>            
            <li><strong>Breakdown at the supplied name and rank: </strong> /breakdown/{collectorytype}/{uid}?rank={rank}&name={name} Example:<br>
            <a href="${initParam.webservicesRoot}/breakdown/dataHubs/dh1?rank=phylum&name=Chordata">/breakdown/dataHubs/dh1?rank=phylum&name=Chordata</a>
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
        	<li><strong>Check for Read Only Mode: </strong><a href="${initParam.webservicesRoot}/admin/isReadOnly">/admin/isReadOnly</a></li>
        	<li><strong>Modify Read Only Mode: </strong>/admin/modify?ro={true OR false}</li>
        	<li><strong>Reopen Index: </strong> /admin/modify?reopenIndex=true</li>
        </ul>

        The remaining services in the section only support POST. All services must supply apiKey as a parameter.

        <ul class="webserviceList">
        	<li><strong>Optimise Index: </strong>/admin/index/optimise - This service will place the biocache-service in read only 
        	mode until the optimise has been completed.
        	<br>Example:<br>
        	curl --data "apiKey=KEY" http://biocache.ala.org.au${initParam.webservicesRoot}/admin/index/optimise
        	</li>
        	<li><strong>Reindex Data Resource: </strong>/admin/index/reindex - reindexes occurrences 
        	modified after startDate for the supplied dataResource<br>Extra Mandatory Parameters: <br>
        	<ul class="paramList">
        		<li>dataResource - The data resource UID to reindex</li>
        		<li>startDate - The earliest modification date to reindex.</li>
        	</ul> 
        	<br>Example:</br>
        	curl --data "apiKey=KEY&dataResource=dr343&startDate=2011-07-01" http://biocache.ala.org.au${initParam.webservicesRoot}/admin/index/reindex       	
			</li>        	
        </ul>
        
        <h3>Miscellaneous</h3>
        <ul class="webserviceList">
        	<li><strong>Retrieve i18n mappings: </strong><a href="${initParam.webservicesRoot}/facets/i18n">/facets/i18n</a> - supports requests for language
                specific properties, such as <a href="${initParam.webservicesRoot}/facets/i18n_fr.properties">/facets/i18n_fr.properties</a> (not currently provided, contact us if required)
            </li>
        	<li><strong>Is Australian test:</strong> /australian/taxon/{guid} - tests to see if the supplied GUID; occurs in Australia, has an Australian LSID or is NOT Australian. Example:<br>
        	<a href="${initParam.webservicesRoot}/australian/taxon/urn:lsid:biodiversity.org.au:afd.taxon:aa745ff0-c776-4d0e-851d-369ba0e6f537">/australian/taxon/urn:lsid:biodiversity.org.au:afd.taxon:aa745ff0-c776-4d0e-851d-369ba0e6f537</a>
        	</li>
        	<li><strong>Images: </strong>/images/taxon/{guid} - obtains a list of occurrence images for the supplied species taxon GUID. Example:<br>
        	<a href="${initParam.webservicesRoot}/images/taxon/urn:lsid:biodiversity.org.au:afd.taxon:dbc44b63-9611-44a8-af58-a29caea777b6">/images/taxon/urn:lsid:biodiversity.org.au:afd.taxon:dbc44b63-9611-44a8-af58-a29caea777b6</a></li>
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
        
        <h3>Mapping Services (/webportal)</h3>
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
                    <li><strong>Construction:</strong> /webportal/params <br>
                        POST service.<br>
                        Stores q and wkt parameters.<br>
                        Returns a short <b>value</b> that can be used as the initial q value in other services for mapping. e.g. q=qid:<b>value</b>
                    </li>
                    <li><strong>Test: </strong> /webportal/params/<b>value</b>
                        Test if a short query parameter is valid.<br>
                        Returns true or false</li>
                </ul>
                </li>
             <li><strong>Occurrences Bounding Box:</strong> /webportal/bbox <br>
                    Returns CSV of bounding box of occurrences</li>
            <li><strong>Data Providers</strong> /webportal/dataProviders </li>
            <li><strong>Species List:</strong>
                <ul>
                    <li><strong>Get species list:</strong> /webportal/species</li>
                    <li><strong>Get species list as CSV:</strong> /webportal/species.csv</li>
                </ul>
            </li>
            <li><strong>Occurrences:</strong>
                <ul>
                    <li><strong>Get occurrences:</strong> /webportal/occurrences</li>
                    <li><strong>Get occurrences as gzipped CSV:</strong> /webportal/occurrences.gz</li>
                </ul>
            </li>
        </ul>

        <h3>WMS Service</h3>
        <p>These services are suitable for use with a OGC client or an OGC friendly API like <a href="http://openlayers.org/">openlayers</a>.
            Examples of use are available <a href="http://spatial.ala.org.au/ws/examples/">here</a>
        </p>

        <ul class="webserviceList">
            <li><strong>Tile:</strong> /webportal/wms/reflect
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
                </ul>
            </li>
            <li><strong>Legend:</strong> /webportal/legend - Get a CSV legend.<br/>
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
            <li><strong>Publication map image</strong> /webportal/wms/image<br/>
                e.g. <a href="${initParam.webservicesRoot}/webportal/wms/image?extents=142,-45,151,-38&q=macropus&format=jpg&dpi=300&pradiusmm=1&popacity=0.8&pcolour=0000FF&widthmm=150&scale=on&baselayer=aus2">/webportal/wms/image?extents=142,-45,151,-38&q=macropus&format=jpg&dpi=300&pradiusmm=1&popacity=0.8&pcolour=0000FF&widthmm=150&scale=on&baselayer=aus2</a>
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
		<ul>
			<li><a href="#outlierInfo" name="outlierInfo" id="outlierInfo"><strong>Outlier test information for species</strong></a><br/>
			e.g. 			
			<a href="${initParam.webservicesRoot}/outlierInfo/urn:lsid:biodiversity.org.au:afd.taxon:0c139726-2add-4abe-a714-df67b1d4b814.json">/outlierInfo/urn:lsid:biodiversity.org.au:afd.taxon:0c139726-2add-4abe-a714-df67b1d4b814.json </a>(Mountain thornbill)
			</li>
			<!--
			<li><a href="#outliers" name="outliers" id="outliers"><strong>Outlier records for species</strong></a><br/>
			e.g. 			
			<a href="${initParam.webservicesRoot}/outliers/urn:lsid:biodiversity.org.au:afd.taxon:0c139726-2add-4abe-a714-df67b1d4b814.json">/outlierInfo/urn:lsid:biodiversity.org.au:afd.taxon:0c139726-2add-4abe-a714-df67b1d4b814.json </a>(Mountain thornbill)
			</li>
			-->
			<li><a href="#outlierRecordDetails" name="outlierRecordDetails" id="outlierRecordDetails"><strong>Outlier record details</strong></a><br/>
			e.g. 			
			<a href="${initParam.webservicesRoot}/outlier/record/b07bbac2-22d7-4c8a-8d61-4be1ab9e0d09">/outlier/record/b07bbac2-22d7-4c8a-8d61-4be1ab9e0d09 </a>(A Mountain thornbill outlier)
			</li>		
		</ul>

        <h2>Free text search of occurrence records (will return JSON)</h2>
		<div id="inpage_search">
			<form id="search-inpage" action="${initParam.webservicesRoot}/occurrences/search" method="get" name="search-form">
			<label for="search">Search</label>
			<input type="text" class="filled ac_input" id="search" name="q" placeholder="Search the Atlas" autocomplete="off">
			<span class="search-button-wrapper"><input type="submit" class="search-button" alt="Search" value="Search"></span>
			</form>
		</div>
    </body>
</html>