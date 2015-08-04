biocache-service [![Build Status](https://travis-ci.org/AtlasOfLivingAustralia/biocache-service.svg?branch=master)](http://travis-ci.org/AtlasOfLivingAustralia/biocache-service)
================

Occurrence &amp; mapping webservices.

Theses services are documented here http://api.ala.org.au/apps/biocache


## Release notes 1.5


## Release notes 1.4


## Release notes 1.3

* ability to switch between name lookup services - allowing switch
* between REST web services provided by BIE and looks against the name
* formatting for readability
* Added filter to make the user-agent request header available to the logger client
* Added dependency: spring-context-suppport
* Fixed pom dependencies
* Added parent pom reference
* Removed unnecessary references from pom
* Added elements to gitignore file
* null checks on download service
* delete records fix
* remove uservoice
* configurable threads for uploads
* added missing i18n
* added old maven repo back in
* removed deploy jar plugin
* removal of redundant jar plugin
* use mavanagaiata plugin for git commit info in artefacts
* exception handling fix around deleted records
* exception handling fix around deleted records
* LinkedHashMap to preserve facets order
* Blank speciesGroup param is ignored
* service exposing species groups config
* species subgroup spatial queries
* prevent duplicates in facet list
* formatting of dynamic facets
* removed redundant code
* missing i18n properties
* configurable facet groupings
* presence / absence facet
* occurrences/facets/download counts
* Fix for endemic/species


## Release notes 1.2

 * git version info which will be visible via view source on the home page
 * Fix for 4326 in WMS, Scatterplot service, Fix for fqs in qids in WMS
 * Revert "Fix for 4326 in WMS, Scatterplot service, Fix for fqs in qids in WMS"
 * Fix for 4326 in WMS, Scatterplot service, Fix for fqs in qids in WMS
 * clazz instead of class
 * prod logging
 * fixes for sounds
 * media store changes + a fix for splitByFacet downloads
 * fixed typo for facet name
 * exception handling
 * fix for regression bug with auth name substitution, and external configuration for biocache media dir and URL substitution
 * removed back spring annotation - overridable with config so release not necessary at this stage
 * Fix for fqs in qids
 * fix for prod logging
 * fix for wmscached

## Release notes 1.1

 * name formatting for themes
 * removed redundant properties from SearchUtils and fixed name index location
 * reverted OccurrenceController commit
 * fix for tempDataResource lookup
 * Added error checking code to be more robust
 * Copy new package structure from branch to trunk
 * Retrofit trunk change to branch
 * more appropriate log4j for tests
 * validation rules fix for backwards compatibility reviewed by DM
 * exclude the servlet-api dependency from biocache-store
 * enhancements to WMS to allow for hiding of certain facets
 * clean up of config
 * package name
 * incremented the version
 * validation rule url
 * old api
 * validation rules URL
 * incremented the version
 * reorganisation of packages
