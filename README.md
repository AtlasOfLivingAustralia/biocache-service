biocache-service
================

Occurrence &amp; mapping webservices.

Theses services are documented here http://api.ala.org.au/apps/biocache

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
