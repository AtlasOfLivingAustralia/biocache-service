package au.org.ala.biocache.dao

import au.org.ala.biocache.dto.DownloadDetailsDTO
import au.org.ala.biocache.dto.DownloadRequestParams
import au.org.ala.biocache.service.DownloadService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.web.WebAppConfiguration
import spock.lang.Specification

import javax.inject.Inject
import javax.inject.Named
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.atomic.AtomicInteger

@ContextConfiguration(locations = "classpath:springTest.xml")
@WebAppConfiguration
class SearchDaoSpec extends Specification {

    @Inject
    SearchDAO searchDao

    @Inject
    DownloadService downloadService

    def 'writeResultsFromIndexToStream test'() {

        setup:
        DownloadRequestParams downloadRequestParams = new DownloadRequestParams()
        downloadRequestParams.dwcHeaders = true
        downloadRequestParams.fields = 'data_resource_uid,modified_date,language,license,rightsholder,access_rights,bibliographic_citation,institution_id,collection_id,dataset_id,institution_code,collection_code,dataset_name,owner_institution_code,basis_of_record,dynamic_properties,occurrence_id,catalogue_number,record_number,collector,individual_count,organism_quantity,organism_quantity_type,raw_sex,life_stage,reproductive_condition,behavior,establishment_means,occurrence_status,preparations,disposition,associated_media,associated_references,associated_sequences,associated_taxa,other_catalog_numbers,occurrence_remarks,previous_identifications,event_id,field_number,occurrence_date,event_time,start_day_of_year,end_day_of_year,year,month,day,verbatim_event_date,habitat,sampling_protocol,sampling_effort,field_notes,event_remarks,location_id,higher_geography,continent,water_body,island_group,island,country,country_code,state,county,municipality,raw_locality,verbatim_locality,min_elevation_d,max_elevation_d,elevation,min_depth_d,max_depth_d,location_according_to,location_remarks,latitude,longitude,coordinate_uncertainty,coordinate_precision,verbatim_coordinates,raw_latitude,verbatim_latitude,raw_longitude,verbatim_longitude,raw_datum,verbatim_coordinate_system,verbatim_srs,footprint_wkt,footprint_srs,georeferenced_by,georeferenced_date,georeference_protocol,georeference_sources,georeference_verification_status,georeference_remarks,identification_id,identification_qualifier,type_status,identified_by,identified_date,identification_references,identification_verification_status,identification_remarks,taxon_id,scientific_name_id,taxon_concept_lsid,raw_taxon_name,taxon_name,accepted_name_usage,parent_name_usage,original_name_usage,name_published_in,higher_classification,kingdom,phylum,class,order,family,genus,subgenus,specific_epithet,infraspecific_epithet,rank,common_name,nomenclatural_code,taxonomic_status,nomenclatural_status,taxon_remarks,individual_id,identifier_role,species,measurement_id,raw_identification_qualifier,provenance,measurement_determined_by,measurement_determined_date,id,rights,source,measurement_value,relationship_remarks,raw_continent,related_resource_id,measurement_accuracy,raw_basis_of_record,measurement_method,relationship_of_resource,measurement_type,measurement_unit,measurement_remarks'

        DownloadDetailsDTO downloadDetailsDTO = new DownloadDetailsDTO()
        downloadDetailsDTO.requestParams = downloadRequestParams

        OutputStream headings = new ByteArrayOutputStream()
        OutputStream data = new ByteArrayOutputStream()

        when:
        ConcurrentMap<String, AtomicInteger> uidStats = searchDao.writeResultsFromIndexToStream(downloadRequestParams, data, false, downloadDetailsDTO, true);

        then:
        uidStats != null
        println "${uidStats}"
        println "--- Data"
        println data.toString()

        expect:

        downloadService.getHeadings(uidStats, headings, downloadRequestParams, downloadDetailsDTO.miscFields)
        println "--- Headings"
        println headings.toString()
    }
}
