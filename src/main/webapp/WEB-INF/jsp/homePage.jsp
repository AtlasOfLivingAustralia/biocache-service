<%@ page contentType="text/html" pageEncoding="UTF-8" %><%@ 
taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" 
%><!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <meta name="pageName" content="home"/>
        <title>BioCache | Atlas of Living Australia</title>
        <style type="text/css">
            .code { font-family: courier new;}
        </style>
    </head>
    <body>
    	<div class="section">
        <h1> Web Services </h1>

        <h3>Occurrences</h3>
        <ul>
            <li><strong>Occurrence listing:</strong>
                <a href="/ws/occurrences/page">/occurrences/page</a>
                - just shows the first 10 occurrences (for debug only)</li>
            <li><strong>Occurrence view:</strong> /occurrence/{uuid}</li>
            <li><strong>Occurrence comparison view:</strong> /occurrence/compare/{uuid}</li>            
            <li><strong>Occurrence for taxon:</strong> /occurrences/taxon/{guid}</li>
            <li><strong>Occurrence for collection:</strong> /occurrences/collections/{uid}</li>
            <li><strong>Occurrence for institution:</strong> /occurrences/institutions/{uid}</li>
            <li><strong>Occurrence for data resource:</strong> /occurrences/dataResources/{uid}</li>
            <li><strong>Occurrence for data provider:</strong> /occurrences/dataProviders/{uid}</li>
            <li><strong>Occurrence for data hub:</strong> /occurrences/dataHubs/{uid}</li>
            <li><strong>Occurrence download:</strong> /occurrences/download - needs request param definition<br>
            The download will include all records that satisfy the q and fq parameters.  The number of records 
            for a data resource may be restricted based on a collectory configured download limit.  Params:
            <ul>
            	<li>q - the initial query</li>
            	<li>fq - filters to be applied to the original query</li>
            	<li>email - the email address of the user requesting the download</li>
            	<li>reason - the reason for the download</li>
            	<li>file - the name to use for the file</li>
            	<li>fields - a CSV list of fields to include in the download (contains a list of default)</li>
            	<li>extra - a CSV list of fields in include in addition to the "fields"</li>
            </ul>
             </li>
            <li><strong>Occurrence wms:</strong> /occurrences/wms - requires WMS parameters along with 'q' and 'fq' populated, if available</li>
            <li><strong>Occurrence static:</strong> /occurrences/static - generates an image of AU with points on, filtering on 'q' and 'fq', if available</li>
            <li><strong>Occurrences coordinates:</strong>
                <a href="/ws/occurrences/coordinates">/occurrences/coordinates</a>
                - Displays a list of unique lat,lon that are used by the occurrences
                 </li>
            <li><strong>List of default facets for occurrence search:</strong> <a href="/ws/search/facets">/search/facets</a></li>
            <li><strong>List of available index fields:</strong> <a href="/ws/index/fields">/index/fields</a> - A field can be used in a search if indexed=true.  A field can be used as a facet if indexed=true and stored=true.</li>
            <li><strong>Facet based download:</strong> /occurrences/facets/download - requires a 'q' and optional 'fq' and one 'facet'. Optional Params:
            <ul>
            	<li>count - set to true if you would like the count included</li>
            	<li>lookup - set to true if you would like the download include the scientific names for the supplied guids.  Downloads that include this param will take extra time as a lookup need to be performed</li>
            </ul> 
            <br>This can be used to download distinct lists of species:
            <ul>
            	<li><a href="/ws/occurrences/facets/download?q=collection_uid:co150&facets=species_guid&lookup=true">/occurrences/facets/download?q=collection_uid:co150&facets=species_guid&lookup=true</a> - downloads a list of species guids and associated scientific names for collection co150</li>
            	<li><a href="/ws/occurrences/facets/download?q=collection_uid:co150&facets=raw_taxon_name">/occurrences/facets/download?q=collection_uid:co150&facets=raw_taxon_name</a> - downloads a list of raw scientific names for collection co150</li>
            	<li><a href="/ws/occurrences/facets/download?q=collection_uid:co150&facets=species_guid&count=true">/occurrences/facets/download?q=collection_uid:co150&facets=species_guid&count=true</a> - downloads a list of species guids and counts for collection co150</li>
            </ul>
            </li>
            <li><strong>Spatial Occurrence search: </strong>/occurrences/spatial - supports point-radius and wkt based searches.  To search by wkt the wkt string can be supplied directly or via a gazetteer URL. Examples:
            	<ul>
            		<li><a href="/ws/occurrences/spatial?lat=-35.27&lon=149.15&radius=10">/occurrences/spatial?lat=-35.27&lon=149.15&radius=10</a></li>
            		<li><a href="/ws/occurrences/spatial?wkt=POLYGON((140:-37,151:-37,151:-26,140.1310:-26,140:-37))">/occurrences/spatial?wkt=POLYGON((140:-37,151:-37,151:-26,140.1310:-26,140:-37))</a></li>
            		<li><a href="/ws/occurrences/spatial?url=http://spatial.ala.org.au/gazetteer/lga/Acton_(Australian_Capital_Territory).xml">/occurrences/spatial?url=http://spatial.ala.org.au/gazetteer/lga/Acton_(Australian_Capital_Territory).xml</a></li>
            	</ul> 
            </li>
            <li><strong>Static Species Density Heatmap </strong><a href="/ws/density/map?q=*:*">/density/map?q=*:*</a></li> - returns heatmap image (optional param forceRefresh=true will regenerate the image)
            <li><strong>Static Species Density Legend: </strong><a href="/ws/density/legend?q=*:*">/density/legend?q=*:*</a></li> - returns associated legend image (optional param forceRefresh=true will regenerate the image)
        </ul>

        <h3>Assertions</h3>
        <ul>
            <li><strong>List assertion codes:</strong>
                <a href="/ws/assertions/codes">/assertions/codes</a>
            </li>
            <li><strong>Add an assertion:</strong> /occurrences/{uuid}/assertions/add</li>
            <li><strong>Add an assertion using params:</strong> /occurrences/assertions/add?recordUuid={uuid}</li>
            <li><strong>Delete an assertion:</strong> /occurrences/{uuid}/assertions/{assertionUuid}/delete</li>
            <li><strong>Delete an assertion using params:</strong> /occurrences/assertions/delete?recordUuid={uuid}&assertionUuid={assertioniUuid}</li>
            <li><strong>List assertions for occurrence:</strong> /occurrences/{uuid}/assertions/</li>
            <li><strong>List assertions for occurrence with params:</strong> /occurrences/assertions?recordUuid={uuid}</li>
        </ul>

        <h3>Breakdowns</h3>
        This section outlines the breakdown services that are available.  These services are available for each of the different collectory types:
        <ul>
        	<li>collections</li>
        	<li>institutions</li>
        	<li>dataResources</li>
        	<li>dataProviders</li>
        	<li>dataHubs</li>
        </ul>
        In the services specified below {collectorytype} must be one of the above values.  
        <ul>
            <li><strong>Breakdown based on limit:</strong> /breakdown/{collectorytype}/{uid}?max={max} Example:<br>
            <a href="/ws/breakdown/collections/co50?max=50">/breakdown/collections/co50?max=50</a> 
            </li>           
            <li><strong>Breakdown of a rank:</strong> /breakdown/{collectorytype}/{uid}?rank={rank} Example:<br>
            <a href="/ws/breakdown/dataResources/dr375?rank=class">/breakdown/dataResources/dr375?rank=class</a>
            </li>            
            <li><strong>Breakdown at the supplied name and rank: </strong> /breakdown/{collectorytype}/{uid}?rank={rank}&name={name} Example:<br>
            <a href="/ws/breakdown/dataHubs/dh1?rank=phylum&name=Chordata">/breakdown/dataHubs/dh1?rank=phylum&name=Chordata</a>
            </li>
        </ul>
        
        <ul>
        	<li><strong>Generic breakdown service is available for any query: </strong> /breakdown - available params
        	 <ul>
            	<li>q - the initial query on which to perform the breakdown</li>
            	<li>fq - filters to be applied to the original query</li>
            	<li>max - the maximum number of names to return for the breakdown.   The rank at which the breakdown will be determined based on this limit.</li>
            	<li>rank - the rank at which to perform the breakdown - if a name is specified in conjunction to this the breakdown will be performed at the next level further limiting the query</li>
            	<li>name - the scientific name to limit the breakdown to - needs a valid rank to be supplied</li>            	
            </ul> 
        	</li>
        </ul>
        
        <h3>Administration</h3>
        
        
        <ul>
        	<li><strong>Check for Read Only Mode: </strong><a href="/ws/admin/isReadOnly">/admin/isReadOnly</a></li>
        	<li><strong>Modify Read Only Mode: </strong>/admin/modify?ro={true OR false}</li>
        	<li><strong>Reopen Index: </strong> /admin/modify?reopenIndex=true</li>
        </ul>
        	The remaining services in the section only support POST. All services must supply apiKey as a parameter.
        <ul>
        	<li><strong>Optimise Index: </strong>/admin/index/optimise - This service will place the biocache-service in read only 
        	mode until the optimise has been completed.
        	<br>Example:<br>
        	curl --data "apiKey=KEY" http://biocache.ala.org.au/ws/admin/index/optimise
        	</li>
        	<li><strong>Reindex Data Resource: </strong>/admin/index/reindex - reindexes occurrences 
        	modified after startDate for the supplied dataResource<br>Extra Mandatory Parameters: <br>
        	<ul>
        		<li>dataResource - The data resource UID to reindex</li>
        		<li>startDate - The earliest modification date to reindex.</li>
        	</ul> 
        	<br>Example:</br>
        	curl --data "apiKey=KEY&dataResource=dr343&startDate=2011-07-01" http://biocache.ala.org.au/ws/admin/index/reindex       	
			</li>        	
        </ul>
        
        <h3>Miscellaneous</h3>
        <ul>
        	<li><strong>Retrieve i18n mappings: </strong><a href="/ws/facets/i18n">/facets/i18n</a></li>
        	<li><strong>Is Australian test:</strong> /australian/taxon/{guid} - tests to see if the supplied GUID; occurs in Australia, has an Australian LSID or is NOT Australian. Example:<br>
        	<a href="/ws/australian/taxon/urn:lsid:biodiversity.org.au:afd.taxon:aa745ff0-c776-4d0e-851d-369ba0e6f537">/australian/taxon/urn:lsid:biodiversity.org.au:afd.taxon:aa745ff0-c776-4d0e-851d-369ba0e6f537</a>
        	</li>
        	<li><strong>Images: </strong>/images/taxon/{guid} - obtains a list of occurrence images for the supplied species taxon GUID. Example:<br>
        	<a href="/ws/images/taxon/urn:lsid:biodiversity.org.au:afd.taxon:dbc44b63-9611-44a8-af58-a29caea777b6">/images/taxon/urn:lsid:biodiversity.org.au:afd.taxon:dbc44b63-9611-44a8-af58-a29caea777b6</a></li>
        </ul>

        <h3>Parsing Webservices</h3>
        <ul>
            <li><strong>Match darwin core terms</strong>: /parse/matchTerms - accepts a POST JSON body. Examples:
                <ul>
                <li><span class="code">["scientific name", "latitude"] </span>
                    <br/><strong>will return</strong><br/>
                    <span  class="code">["scientificName", "decimalLatitude"]</span>
                </li>
                <li><span class="code"> ["Macropus rufus", "12.2", "149.0"] </span>
                    <br/><strong>will return</strong><br/>
                    <span class="code">["species", "decimalLatitude","decimalLongitude"] </span>
                </li>
                </ul>
            </li>
            <li><strong>Are darwin core terms? </strong>: /parse/areDwcTerms - accepts a POST JSON body. Examples:
                <ul>
                <li><span class="code">["scientific name", "latitude"]</span>
                    <br/><strong>will return</strong><br/>
                    <span class="code">true</span></li>
                <li><span class="code">["Macropus rufus", "12.2", "149.0"]</span>
                    <br/><strong>will return</strong><br/>
                    <span class="code">false</span></li>
                </ul>
            </li>
            <li><strong>Ad hoc processing</strong>: /process/adhoc - accepts a POST JSON body. Examples:
                <ul>
                <li><span class="code">{ "scientificName": "Macropus rufus" }</span>
                    <br/><strong>will return</strong><br/>
                    <span class="code">
{"assertions":[{"comment":"Missing basis of record","problemAsserted":true,"code":20001,"name":"missingBasisOfRecord","uuid":"bf9dc7a0-9918-4ae1-ac2c-2cd9729a1dc0"},{"comment":"No date information supplied","problemAsserted":true,"code":30008,"name":"missingCollectionDate","uuid":"fe473a55-89ca-4803-b8b3-c050b930d287"}],"values":[{"processed":"Macropus rufus","raw":"","name":"species"},{"processed":"urn:lsid:biodiversity.org.au:afd.taxon:065f1da4-53cd-40b8-a396-80fa5c74dedd","raw":"","name":"phylumID"},{"processed":"urn:lsid:biodiversity.org.au:afd.taxon:e9e7db31-04df-41fb-bd8d-e0b0f3c332d6","raw":"","name":"classID"},{"processed":"urn:lsid:biodiversity.org.au:afd.taxon:6d8079f1-edc9-4aab-aabd-232a32b42471","raw":"","name":"orderID"},{"processed":"Mammalia","raw":"","name":"classs"},{"processed":"urn:lsid:biodiversity.org.au:afd.taxon:72a1c39f-2435-4c28-a680-714b69ded6f9","raw":"","name":"familyID"},{"processed":"2161700","raw":"","name":"left"},{"processed":"Chordata","raw":"","name":"phylum"},{"processed":"species","raw":"","name":"taxonRank"},{"processed":"Animalia","raw":"","name":"kingdom"},{"processed":"Macropus rufus","raw":"Macropus rufus","name":"scientificName"},{"processed":"Macropus","raw":"","name":"genus"},{"processed":"7000","raw":"","name":"taxonRankID"},{"processed":"Diprotodontia","raw":"","name":"order"},{"processed":"urn:lsid:biodiversity.org.au:afd.taxon:aa745ff0-c776-4d0e-851d-369ba0e6f537","raw":"","name":"taxonConceptID"},{"processed":"[\"Animals\",\"Mammals\"]","raw":"","name":"speciesGroups"},{"processed":"Macropodidae","raw":"","name":"family"},{"processed":"urn:lsid:biodiversity.org.au:afd.taxon:aa745ff0-c776-4d0e-851d-369ba0e6f537","raw":"","name":"speciesID"},{"processed":"urn:lsid:biodiversity.org.au:afd.taxon:558a729a-789b-4b00-a685-8843dc447319","raw":"","name":"genusID"},{"processed":"2161701","raw":"","name":"right"},{"processed":"urn:lsid:biodiversity.org.au:afd.taxon:4647863b-760d-4b59-aaa1-502c8cdf8d3c","raw":"","name":"kingdomID"},{"processed":"Red Kangaroo","raw":"","name":"vernacularName"}]}
                    </span>
                </li>
                </ul>
            </li>
        </ul>

        <h2>Free text search of occurrence records (will return JSON)</h2>
		<div id="inpage_search">
			<form id="search-inpage" action="/ws/occurrences/search" method="get" name="search-form">
			<label for="search">Search</label>
			<input type="text" class="filled ac_input" id="search" name="q" placeholder="Search the Atlas" autocomplete="off">
			<span class="search-button-wrapper"><input type="submit" class="search-button" alt="Search" value="Search"></span>
			</form>
		</div>
    </body>
</html>