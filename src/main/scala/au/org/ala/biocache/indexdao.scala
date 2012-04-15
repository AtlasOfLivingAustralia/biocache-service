/*
 * The configuration required for the SOLR index
 */
package au.org.ala.biocache

import java.lang.reflect.Method
import java.util.Collections
import org.apache.commons.lang.time.DateUtils
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer
import org.apache.solr.client.solrj.SolrServer
import org.apache.solr.common.SolrInputDocument
import org.apache.solr.core.CoreContainer
import org.slf4j.LoggerFactory
import com.google.inject.Inject
import com.google.inject.name.Named
import scala.actors.Actor
import scala.collection.mutable.ArrayBuffer
import java.util.Date
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.time.DateFormatUtils
import java.io.{File, OutputStream}
import java.util.concurrent.ArrayBlockingQueue


/**
 * All Index implementations need to extend this trait.
 */
trait IndexDAO {

    import org.apache.commons.lang.StringUtils.defaultString
    val elFields = Config.fieldsToSample.filter(x => x.startsWith("el"))
    val clFields = Config.fieldsToSample.filter(x => x.startsWith("cl"))

    def getRowKeysForQuery(query:String, limit:Int=1000):Option[List[String]]
    
    def writeRowKeysToStream(query:String, outputStream: OutputStream)

    def occurrenceDAO:OccurrenceDAO
    
    def getDistinctValues(query:String,field:String,max:Int):Option[List[String]]

    def pageOverFacet(proc:(String,Int) => Boolean, facetName:String, query:String, filterQueries:Array[String])

    def pageOverIndex(proc:java.util.Map[String,AnyRef] => Boolean, fieldToRetrieve:Array[String], query:String, filterQueries:Array[String])

    /**
     * Index a record with the supplied properties.
     */
    def indexFromMap(guid: String, map: Map[String, String], batch:Boolean=true,startDate:Option[Date]=None)

    /**
     * Truncate the current index
     */
    def emptyIndex
    def reload
    def shutdown
    def optimise :String
    /**
     * Remove all the records with the specified value in the specified field
     */
    def removeFromIndex(field:String, values:String)
    /** Deletes all the records that satisfy the supplied query*/
    def removeByQuery(query:String, commit:Boolean=true)

    /**
     * Perform
     */
    def finaliseIndex(optimise:Boolean=false, shutdown:Boolean=true)

    def getValue(field: String, map: Map[String, String]) : String = {
        val value = map.get(field)
        if (!value.isEmpty){
            return value.get
        } else {
           return  ""
        }
    }

    def getValue(field: String, map: Map[String, String], checkparsed:Boolean):String ={
       var value = getValue(field, map)
       if(value == "" && checkparsed)
         value = getValue(field + ".p", map)
       value
    }

    /**
     * Returns an array of all the assertions that are in the Map.
     * This duplicates some of the code that is in OccurrenceDAO because
     * we are not interested in processing the other values
     *
     * TODO we may wish to fix this so that it uses the same code in the mappers
     */
    def getAssertions(map: Map[String, String]): Array[String] = {

        val columns = map.keySet
        val buff = new ArrayBuffer[String]
        columns.foreach(fieldName =>
            if (FullRecordMapper.isQualityAssertion(fieldName)) {
                val value = map.get(fieldName).get
                if(value != "true" && value != "false"){
                  Json.toIntArray(value).foreach(code =>{
                    val codeOption = AssertionCodes.getByCode(code)
                    if(!codeOption.isEmpty){
                      buff += codeOption.get.getName
                    }
                  })
                }
            }
        )
        buff.toArray
    }

    /**
     * Returns a lat,long string expression formatted to the supplied Double format
     */
    def getLatLongString(lat: Double, lon: Double, format: String): String = {
        if (!lat.isNaN && !lon.isNaN) {
            val df = new java.text.DecimalFormat(format)
            df.format(lat) + "," + df.format(lon)
        } else {
            ""
        }
    }

    /**
     * The header values for the CSV file.
     */
    val header = List("id", "row_key", "occurrence_id", "data_hub_uid", "data_hub", "data_provider_uid", "data_provider", "data_resource_uid",
      "data_resource", "institution_uid", "institution_code", "institution_name",
      "collection_uid", "collection_code", "collection_name", "catalogue_number",
      "taxon_concept_lsid", "occurrence_date", "occurrence_year", "taxon_name", "common_name", "names_and_lsid", "common_name_and_lsid",
      "rank", "rank_id", "raw_taxon_name", "raw_common_name", "multimedia", "image_url", "all_image_url",
      "species_group", "country_code", "country", "lft", "rgt", "kingdom", "phylum", "class", "order",
      "family", "genus", "genus_guid", "species","species_guid", "state", "imcra", "ibra", "places", "latitude", "longitude",
      "lat_long", "point-1", "point-0.1", "point-0.01", "point-0.001", "point-0.0001",
      "year", "month", "basis_of_record", "raw_basis_of_record", "type_status",
      "raw_type_status", "taxonomic_kosher", "geospatial_kosher", "assertions", "location_remarks",
      "occurrence_remarks", "citation", "user_assertions", "system_assertions", "collector","state_conservation","raw_state_conservation",
      "sensitive", "coordinate_uncertainty", "user_id","provenance","subspecies_guid", "subspecies_name", "interaction","last_assertion_date",
      "last_load_date","last_processed_date", "modified_date", "establishment_means","loan_number","loan_identifier","loan_destination",
      "loan_botanist","loan_date", "loan_return_date","original_name_usage","duplicate_inst", "record_number","first_loaded_date","name_match_metric",
      "life_stage", "outlier_layer", "outlier_layer_count", "taxonomic_issue","raw_identification_qualifier","species_habitats",
      "identified_by","identified_date") // ++ elFields ++ clFields

  /**
   * Constructs a scientific name.
   *
   * TODO Factor this out of indexing logic, and have a separate field in cassandra that stores this.
   * TODO Construction of this field can then happen as part of the processing.
   */
  def getRawScientificName(map: Map[String, String]): String = {
    val scientificName: String = {
      if (map.contains("scientificName"))
        map.get("scientificName").get
      else if (map.contains("genus")) {
        var tmp: String = map.get("genus").get
        if (map.contains("specificEpithet") || map.contains("species")) {
          tmp = tmp + " " + map.getOrElse("specificEpithet", map.getOrElse("species", ""))
          if (map.contains("infraspecificEpithet") || map.contains("subspecies"))
            tmp = tmp + " " + map.getOrElse("infraspecificEpithet", map.getOrElse("subspecies", ""))
        }
        tmp
      }
      else
        map.getOrElse("family", "")
    }
    scientificName
  }
            
    /**
     * Generates an string array version of the occurrence model.
     *
     * Access to the values are taken directly from the Map with no reflection. This
     * should result in a quicker load time.
     *
     */
    def getOccIndexModel(guid: String, map: Map[String, String]): List[String] = {

        try {
            //get the lat lon values so that we can determine all the point values
            val deleted = getValue(FullRecordMapper.deletedColumn, map)
            //only add it to the index is it is not deleted
            if (!deleted.equals("true") && map.size>1) {
                var slat = getValue("decimalLatitude.p", map)
                var slon = getValue("decimalLongitude.p", map)
                var latlon = ""
                val sciName = getValue("scientificName.p", map)
                val taxonConceptId = getValue("taxonConceptID.p", map)
                val vernacularName = getValue("vernacularName.p", map).trim
                val kingdom = getValue("kingdom.p", map)
                val family = getValue("family.p", map)
                val images = {
                    val simages = getValue("images.p", map)
                    if (simages.length > 0)
                        Json.toStringArray(simages)
                    else
                        Array[String]()
                }
                //determine the type of multimedia that is available.
                val multimedia:Array[String] ={
                  val i = map.getOrElse("images.p","[]")
                  val s = map.getOrElse("sounds.p","[]")
                  val v = map.getOrElse("videos.p","[]")
                  val ab = new ArrayBuffer[String]
                  if(i.length()>3) ab + "Image"
                  if(s.length()>3) ab + "Sound"
                  if(v.length()>3) ab + "Video"
                  if(ab.size>0)
                    ab.toArray
                  else
                    Array("None")
                    
                }
                val speciesGroup = {
                    val sspeciesGroup = getValue("speciesGroups.p", map)
                    if (sspeciesGroup.length > 0)
                        Json.toStringArray(sspeciesGroup)
                    else
                        Array[String]()
                }
                val interactions = {
                    if(map.contains("interactions.p")){
                        Json.toStringArray(map.get("interactions.p").get)
                    }
                    else
                        Array[String]()
                }
                val dataHubUids = {
                    val sdatahubs = getValue("dataHubUid", map, true)
                    if (sdatahubs.length > 0)
                        Json.toStringArray(sdatahubs)
                    else
                        Array[String]()
                }
                val habitats = {
                  val shab = map.getOrElse("speciesHabitats.p","[]")
                  Json.toStringArray(shab)
                }
                
                var eventDate = getValue("eventDate.p", map)
                var occurrenceYear = getValue("year.p", map)
                if (occurrenceYear.length == 4)
                    occurrenceYear += "-01-01T00:00:00Z"
                else
                    occurrenceYear = ""
                //only want to include eventDates that are in the correct format
                try {
                    DateUtils.parseDate(eventDate, Array("yyyy-MM-dd"))
                }
                catch {
                    case e: Exception => eventDate = ""
                }
                var lat = java.lang.Double.NaN
                var lon = java.lang.Double.NaN

                if (slat != "" && slon != "") {
                    try {
                        lat = java.lang.Double.parseDouble(slat)
                        lon = java.lang.Double.parseDouble(slon)
                        val test = -90D
                        val test2 = -180D
                        //ensure that the lat longs are in the required range before
                        if( lat<=90  && lat >= test && lon <=180 && lon>=test2 ){
                          latlon = slat + "," + slon
                        }
                    } catch {
                        //If the latitude or longitude can't be parsed into a double we don't want to index the values
                        case e: Exception => slat = ""; slon = ""
                    }
                }
                val sconservation = getValue("stateConservation.p", map)
                var stateCons = if(sconservation!="") sconservation.split(",")(0) else ""
                val rawStateCons = if(sconservation!="") sconservation.split(",")(1) else ""
                if(stateCons == "null") stateCons = rawStateCons;
                
                val sensitive:String = {
                    if(getValue("originalSensitiveValues",map) != "" || getValue("originalDecimalLatitude",map) != "") {
                        val dataGen = map.getOrElse("dataGeneralizations.p", "")
                        if(dataGen.contains("already generalised"))
                            "alreadyGeneralised"
                        else if(dataGen != "")
                            "generalised"
                        else
                            ""
                    }
                    else
                        ""
                }

                val outlierForLayers:Array[String] = {
                  val outlierForLayerStr = getValue("outlierForLayers.p", map)
                  if(outlierForLayerStr != "") Json.toStringArray(outlierForLayerStr)
                  else Array()
                }              

                //Only set the geospatially kosher field if there are coordinates supplied
                val geoKosher = if(slat == "" && slon == "") "" else map.getOrElse(FullRecordMapper.geospatialDecisionColumn, "")
                val hasUserAss = map.getOrElse(FullRecordMapper.userQualityAssertionColumn, "") match {
                    case "true" => "true"
                    case "false" =>"false"
                    case value:String => (value.length >3).toString
                }
                val (subspeciesGuid, subspeciesName):(String,String) = {
                    if(map.contains("taxonRankID.p")){
                        try{
                         if(java.lang.Integer.parseInt(map.getOrElse("taxonRankID.p","")) > 7000)
                           (taxonConceptId, sciName)
                         else
                           ("","")
                        }
                        catch {
                            case _ => ("","")
                        }
                    }
                    else ("","")
                }
                
                val lastLoaded = DateParser.parseStringToDate(getValue(FullRecordMapper.alaModifiedColumn,map))
                
                val lastProcessed = DateParser.parseStringToDate(getValue(FullRecordMapper.alaModifiedColumn+".p",map))
                
                val lastUserAssertion = DateParser.parseStringToDate(map.getOrElse(FullRecordMapper.lastUserAssertionDateColumn, ""))
                
                val firstLoadDate = DateParser.parseStringToDate(getValue("firstLoaded",map))
                //get the el and cl maps to work with
//                val elmap =map.getOrElse("el.p","").dropRight(1).drop(1).split(",").map(_ split ":") collect {case Array(k,v) => (k.substring(1,k.length-1), v.substring(1,v.length-1))} toMap
//                val clmap = map.getOrElse("cl.p", "").dropRight(1).drop(1).split(",").map(_ split ":") collect {case Array(k,v) => (k.substring(1,k.length-1), v.substring(1,v.length-1))} toMap

                return List(getValue("uuid", map),
                    getValue("rowKey", map),
                    getValue("occurrenceID", map),
                    dataHubUids.mkString("|"),
                    getValue("dataHub.p", map),
                    getValue("dataProviderUid", map, true),
                    getValue("dataProviderName", map, true),
                    getValue("dataResourceUid", map, true),
                    getValue("dataResourceName", map, true),
                    getValue("institutionUid.p", map),
                    getValue("institutionCode", map),
                    getValue("institutionName.p", map),
                    getValue("collectionUid.p", map),
                    getValue("collectionCode", map),
                    getValue("collectionName.p", map),
                    getValue("catalogNumber", map),
                    taxonConceptId, if (eventDate != "") eventDate + "T00:00:00Z" else "", occurrenceYear,
                    sciName,
                    vernacularName,
                    sciName + "|" + taxonConceptId + "|" + vernacularName + "|" + kingdom + "|" + family,
                    vernacularName+ "|"+sciName + "|" + taxonConceptId + "|" + vernacularName + "|" + kingdom + "|" + family,
                    getValue("taxonRank.p", map),
                    getValue("taxonRankID.p", map),
                    getRawScientificName(map),
                    getValue("vernacularName", map),
                    //if (!images.isEmpty && images(0) != "") "Multimedia" else "None",
                    multimedia.mkString("|"),
                    if (!images.isEmpty) images(0) else "",
                    images.mkString("|"),
                    speciesGroup.mkString("|"),
                    getValue("countryCode", map),
                    getValue("country.p", map),
                    getValue("left.p", map),
                    getValue("right.p", map),
                    kingdom, getValue("phylum.p", map),
                    getValue("classs.p", map),
                    getValue("order.p", map), family,
                    getValue("genus.p", map),
                    map.getOrElse("genusID.p", ""),
                    getValue("species.p", map),
                    getValue("speciesID.p",map),                    
                    map.getOrElse("stateProvince.p", ""),              
                    getValue("imcra.p", map),
                    getValue("ibra.p", map),
                    getValue("lga.p", map),
                    slat,
                    slon,
                    latlon,
                    getLatLongString(lat, lon, "#"),
                    getLatLongString(lat, lon, "#.#"),
                    getLatLongString(lat, lon, "#.##"),
                    getLatLongString(lat, lon, "#.###"),
                    getLatLongString(lat, lon, "#.####"),
                    getValue("year.p", map),
                    getValue("month.p", map),
                    getValue("basisOfRecord.p", map),
                    getValue("basisOfRecord", map),
                    getValue("typeStatus.p", map),
                    getValue("typeStatus", map),
                    getValue(FullRecordMapper.taxonomicDecisionColumn, map),
                    geoKosher,
                    getAssertions(map).mkString("|"),
                    getValue("locationRemarks", map),
                    getValue("occurrenceRemarks", map),
                    "",
                    hasUserAss,
                    (getValue(FullRecordMapper.qualityAssertionColumn, map).length > 3).toString,
                    getValue("recordedBy", map),
                    //getValue("austConservation.p",map),
                    stateCons,
                    rawStateCons,
                    sensitive,
                    getValue("coordinateUncertaintyInMeters.p",map),
                    map.getOrElse("recordedBy", ""), map.getOrElse("provenance.p",""), subspeciesGuid, subspeciesName,
                    interactions.mkString("|"),
                    if(lastUserAssertion.isEmpty)"" else DateFormatUtils.format(lastUserAssertion.get,"yyyy-MM-dd'T'HH:mm:ss'Z'"), 
                    if(lastLoaded.isEmpty)"2010-11-1T00:00:00Z" else DateFormatUtils.format(lastLoaded.get, "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                    if(lastProcessed.isEmpty)"" else DateFormatUtils.format(lastProcessed.get,"yyyy-MM-dd'T'HH:mm:ss'Z'"),
                    if(map.contains("modified.p")) map.getOrElse("modified.p","") + "T00:00:00Z" else "",
                    map.getOrElse("establishmentMeans.p","").replaceAll(",","|"),
                    map.getOrElse("loanSequenceNumber", ""), map.getOrElse("loanIdentifier", ""), map.getOrElse("loanDestination",""),
                    map.getOrElse("loanForBotanist",""), 
                    if(map.contains("loanDate")) map.getOrElse("loanDate","") + "T00:00:00Z" else "",
                    if(map.contains("loanReturnDate")) map.getOrElse("loanReturnDate","") + "T00:00:00Z" else "",
                    map.getOrElse("originalNameUsage", map.getOrElse("typifiedName","")),
                    map.getOrElse("duplicates",""),//.replaceAll(",","|"),
                    map.getOrElse("recordNumber",""),
                    if(firstLoadDate.isEmpty) "" else DateFormatUtils.format(firstLoadDate.get, "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                    map.getOrElse("nameMatchMetric.p", ""),
                    map.getOrElse("phenology",""),  //TODO make this a controlled vocab that gets mapped during processing...                    
                    outlierForLayers.mkString("|"),
                    outlierForLayers.length.toString, map.getOrElse("taxonomicIssue.p",""), map.getOrElse("identificationQualifier",""),
                    habitats.mkString("|"), map.getOrElse("identifiedBy",""), 
                    if(map.contains("dateIdentified.p")) map.getOrElse("dateIdentified.p","") + "T00:00:00Z" else ""
                    ) //++ elFields.map(field => elmap.getOrElse(field,"")) ++ clFields.map(field=> clmap.getOrElse(field,"")
                //)
            }
            else {
                return List()
            }
        }
        catch {
            case e: Exception => e.printStackTrace; throw e
        }
    }
}
/**
 * An class for handling a generic/common index fields
 * 
 * Not in use yet.
 */
class IndexField(fieldName:String, dataType:String, sourceField:String, multi:Boolean=false, storeAsArray:Boolean=false, extraField:Option[String]=None){
    
    def getValuesForIndex(map: Map[String, String]):(String,Option[Array[String]])={
        //get the source value. Cater for the situation where we get the parsed value if raw doesn't exist
        val sourceValue:String = {
            if(sourceField.contains(",")){
                //There are multiple fields that supply the source for the field
                val fields = sourceField.split(",")
                fields.foldLeft("")((concat,value)=>concat + "|" + map.getOrElse(value, ""))                
            } 
            else map.getOrElse(sourceField, if(extraField.isDefined)map.getOrElse(extraField.get,"") else "")
            
        }
        dataType match {
            case "date" =>{
                 val date =DateParser.parseStringToDate(sourceValue)
                 if(date.isDefined)
                     return (fieldName,Some(Array(DateFormatUtils.format(date.get, "yyyy-MM-dd'T'HH:mm:ss'Z'"))))
            }
            case "double" =>{
                //needs to be a valid double
                try{
                    java.lang.Double.parseDouble(sourceValue)
                    return (fieldName, Some(Array(sourceValue)))
                }
                catch{
                    case _=>(fieldName,None)
                }
            }
            case _ =>{
                if(sourceValue.length>0){
                    if(multi && storeAsArray){                                               
                         val array = Json.toStringArray(sourceValue)
                         return (fieldName, Some(array))                        
                    }
                    if(multi){
                        return (fieldName, Some(sourceValue.split(",")))
                    }
                    
                }
             }
        }
        (fieldName,None)
    }
}

/**
 * DAO for indexing to SOLR
 */
class SolrIndexDAO @Inject()(@Named("solrHome") solrHome:String) extends IndexDAO {
    import org.apache.commons.lang.StringUtils.defaultString
    import scalaj.collection.Imports._

    //val cc = new CoreContainer.Initializer().initialize
    var cc:CoreContainer = _
    var solrServer:SolrServer =_ //new EmbeddedSolrServer(cc, "")
    var solrConfigPath:String = ""

    @Inject
    var occurrenceDAO:OccurrenceDAO = _

    val logger = LoggerFactory.getLogger("SolrOccurrenceDAO")
    val solrDocList = new java.util.ArrayList[SolrInputDocument](1000)
    
    //val thread = new SolrIndexActor()
    val docQueue: ArrayBlockingQueue[java.util.List[SolrInputDocument]] = new ArrayBlockingQueue[java.util.List[SolrInputDocument]](2);
    var ids =0
    lazy val threads:Array[AddDocThread] ={
      Array.fill[AddDocThread](1){ val t = new AddDocThread(docQueue,ids); ids +=1; t.start;t}
    }

    def init(){
      if(solrServer == null){
        if (solrConfigPath != ""){
          //System.setProperty("solr.solr.home", solrHome)
          println("Initialising SOLR with config path: " + solrConfigPath +", and SOLR HOME: " + solrHome)
          val solrConfigFile = new File(solrConfigPath)
          val home = solrConfigFile.getParentFile.getParentFile
          val f = new File(solrConfigFile.getParentFile.getParentFile , "solr.xml" );
          cc = new CoreContainer();
          cc.load(home.getAbsolutePath, f );
          solrServer  = new EmbeddedSolrServer( cc, "biocache" );
          //thread.start
          threads
        } else {
          System.setProperty("solr.solr.home", solrHome)
          val initializer = new CoreContainer.Initializer()
          cc = initializer.initialize
          solrServer = new EmbeddedSolrServer(cc, "biocache")
          //thread.start
          threads
        }
      }
    }

    def reload = cc.reload("")

  def pageOverFacet(proc:(String,Int) => Boolean, facetName:String, queryString:String = "*:*", filterQueries:Array[String] = Array()){
    init
    var query:SolrQuery = new SolrQuery(queryString)
    query.setFacet(true)
    query.addFacetField(facetName)
    query.setRows(0)
    query.setFacetLimit(200000)
    query.setStart(0)
    if (!filterQueries.isEmpty) query.setFilterQueries(filterQueries: _ *)
      
    var response = solrServer.query(query)
    response.getFacetField(facetName).getValues.foreach(s => proc(s.getName, s.getCount.toInt))
  }


  def pageOverIndex(proc:java.util.Map[String,AnyRef] => Boolean, fieldToRetrieve:Array[String], queryString:String = "*:*", filterQueries:Array[String] = Array()){
    init


    var startIndex = 0
    var query:SolrQuery = new SolrQuery(queryString)
    query.setFacet(false)
    query.setRows(0)
    query.setStart(startIndex)
    query.setFilterQueries(filterQueries: _ *)
    query.setFacet(false)
    fieldToRetrieve.foreach(f => query.addField(f))
    var response = solrServer.query(query)
    val fullResults = response.getResults.getNumFound.toInt
    println("Total found for :" + queryString +", " + fullResults)

    var counter = 0
    var pageSize = 5000
    while(counter < fullResults){

      var q:SolrQuery = new SolrQuery(queryString)
      q.setFacet(false)
      q.setStart(counter)
      q.setFilterQueries(filterQueries: _ *)
      q.setFacet(false)

      if(counter + pageSize > fullResults){
        pageSize = fullResults - counter
      }

      //setup the next query
      q.setRows(pageSize)
      response = solrServer.query(q)
      println("Paging through :" + queryString +", " + counter)
      val solrDocumentList = response.getResults
      val iter = solrDocumentList.iterator()
      while(iter.hasNext){
        val solrDocument = iter.next()
        proc(solrDocument.getFieldValueMap)
      }
      counter += pageSize
    }
  }

//    def XXXXXpageOverIndex(proc:java.util.Map[String,AnyRef] => Boolean, fieldToRetrieve:Array[String], queryString:String = "*:*", filterQueries:Array[String] = Array()){
//      init
//
//      var pageSize = 0
//      var startIndex = 0
//      var query:SolrQuery = new SolrQuery(queryString)
//      query.setFacet(false)
//      query.setRows(pageSize)
//      query.setStart(startIndex)
//      fieldToRetrieve.foreach(f => query.addField(f))
//      var response = solrServer.query(query)
//
//      val fullResults = response.getResults.getNumFound
//      println("Total found for :" + queryString +", " + fullResults)
//      query.setRows(fullResults.toInt)
//      query.setFacet(false)
//      query.setFilterQueries(filterQueries: _ *)
//      response = solrServer.query(query)
//
//
//
//      val solrDocumentList = response.getResults
//      val iter = solrDocumentList.iterator()
//      while(iter.hasNext){
//        val solrDocument = iter.next()
//        proc(solrDocument.getFieldValueMap)
//      }
//    }

    def emptyIndex() {
        init
        try {
        	solrServer.deleteByQuery("*:*")
        } catch {
          case e:Exception => e.printStackTrace(); println("Problem clearing index...")
        }
    }
    

    def removeFromIndex(field:String, value:String) ={
        init
        try{
          //println("Deleting " + field +":" + value)
            solrServer.deleteByQuery(field +":\"" + value+"\"")
            solrServer.commit
        }
        catch{
            case e:Exception => e.printStackTrace
        }
    }
    
    def removeByQuery(query:String, commit:Boolean = true) ={
        init
        logger.debug("Deleting by query: " + query)
        try{
            solrServer.deleteByQuery(query)
            if(commit)
                solrServer.commit
        }
        catch{
            case e:Exception =>e.printStackTrace
        }
    }

    def finaliseIndex(optimise:Boolean=false, shutdown:Boolean=true) {
        init
        if (!solrDocList.isEmpty) {
        
           //SolrIndexDAO.solrServer.add(solrDocList)
//           while(!thread.ready){ Thread.sleep(50) }
//           thread ! solrDocList
          while(docQueue.size()>1){Thread.sleep(250)}
          docQueue.add(solrDocList)
           
           //wait enough time for the Actor to get the message
          Thread.sleep(50)
        }
        //while(!thread.ready){ Thread.sleep(50) }
        while(docQueue.size > 0){Thread.sleep(50)}
        solrServer.commit
        solrDocList.clear
        println(printNumDocumentsInIndex)
        if(optimise){
          println("Optimising the indexing...")
          this.optimise
        }
        if(shutdown){
          println("Shutting down the indexing...")
        	this.shutdown
        }
        println("Finalise finished.")
    }
    /**
     * Shutdown the index by stopping the indexing thread and shutting down the index core
     */
    def shutdown() = {
        //thread ! "exit"
      threads.foreach(t => t.stopRunning)
      if(cc != null)
        	cc.shutdown
    }
    def optimise():String = {
        init
        solrServer.optimize
        printNumDocumentsInIndex
    }

    /**
     * Decides whether or not the current record should be indexed based on processed times
     */
    def shouldIndex(map:Map[String,String], startDate:Option[Date]):Boolean={
        if(!startDate.isEmpty){            
            val lastLoaded = DateParser.parseStringToDate(getValue(FullRecordMapper.alaModifiedColumn,map))
            val lastProcessed = DateParser.parseStringToDate(getValue(FullRecordMapper.alaModifiedColumn+".p",map))
            return startDate.get.before(lastProcessed.getOrElse(startDate.get)) || startDate.get.before(lastLoaded.getOrElse(startDate.get))
        }        
        true
    }

    /**
     * A SOLR specific implementation of indexing from a map.
     */
  override def indexFromMap(guid: String, map: Map[String, String], batch: Boolean = true, startDate: Option[Date] = None) = {
    init
    //val header = getHeaderValues()
    if (shouldIndex(map, startDate)) {
      val values = getOccIndexModel(guid, map)
      if (values.length > 0 && values.length != header.length) {
        println("values don't matcher header: " + values.length + ":" + header.length + ", values:header")
        println(header)
        println(values)
        exit(1)
      }
      if (values.length > 0) {
        val doc = new SolrInputDocument()
        for (i <- 0 to values.length - 1) {
          if (values(i) != "") {
            if (header(i) == "duplicate_inst" || header(i) == "establishment_means" || header(i) == "species_group"
              || header(i) == "assertions" || header(i) == "data_hub_uid" || header(i) == "interactions"
              || header(i) == "outlier_layer" || header(i) == "species_habitats" || header(i) == "multimedia" || header(i) == "all_image_url") {
              //multiple values in this field
              for (value <- values(i).split('|')) {
                if (value != "")
                  doc.addField(header(i), value)
              }
            }
            else
              doc.addField(header(i), values(i))
          }
        }
        
        //now index the QA names for the user if userQA = true
        val hasUserAssertions = map.getOrElse(FullRecordMapper.userQualityAssertionColumn, "false")
        if("true".equals(hasUserAssertions)){
        	val assertionUserIds = Config.occurrenceDAO.getUserIdsForAssertions(guid)
            assertionUserIds.foreach(id => doc.addField("assertion_user_id",id))
        }
        
        //index the available el and cl's - more efficient to use the supplied map than using the old way 
        val els = Json.toStringMap(map.getOrElse("el.p", "{}"))      
        els.foreach{case (key,value) => doc.addField(key, value)}
        val cls =  Json.toStringMap(map.getOrElse("cl.p", "{}"))        
        cls.foreach{case (key,value) => doc.addField(key, value)}
        
        //TODO Think about moving species group stuff here.
        //index the additional species information - ie species groups
//        val lft = map.get("left.p")
//        val rgt = map.get("right.p")
//        if(lft.isDefined && rgt.isDefined){
//          val sgs = SpeciesGroups.getSpeciesGroups(lft.get, rgt.get)
//          if(sgs.isDefined){
//            sgs.get.foreach{v:String => doc.addField("species_group", v)}
//          }
//        }
        

        if (!batch) {
          solrServer.add(doc);
          solrServer.commit
        }
        else {

          if (!StringUtils.isEmpty(values(0)))
            solrDocList.add(doc)

          if (solrDocList.size == 10000) {
//            while (!thread.ready) {
//              Thread.sleep(50)
//            }

            val tmpDocList = new java.util.ArrayList[SolrInputDocument](solrDocList);
            while(docQueue.size()>1){Thread.sleep(250)}
            docQueue.add(tmpDocList)
            //thread ! tmpDocList
            solrDocList.clear
          }
       }
      }
    }
  }

  /**
   * Gets the rowKeys for the query that is supplied
   * Do here so that still works if web service is down
   *
   * This causes OOM exceptions at SOLR for large numbers of row keys
   * Use writeRowKeysToStream instead
   */
  override def getRowKeysForQuery(query: String, limit: Int = 1000): Option[List[String]] = {
    init
    val solrQuery = new SolrQuery();
    solrQuery.setQueryType("standard");
    // Facets
    solrQuery.setFacet(true)
    solrQuery.addFacetField("row_key")
    solrQuery.setQuery(query)
    solrQuery.setRows(0)
    solrQuery.setFacetLimit(-1)
    solrQuery.setFacetMinCount(1)
    val response = solrServer.query(solrQuery)
    logger.debug("query " + solrQuery.toString)
    //now process all the values that are in the row_key facet
    val rowKeyFacets = response.getFacetField("row_key")
    val values = rowKeyFacets.getValues().asScala[org.apache.solr.client.solrj.response.FacetField.Count]
    if (values.size > 0) {
      Some(values.map(facet => facet.getName).asInstanceOf[List[String]]);
    }
    else
      None
  }
  
  def getDistinctValues(query:String,field:String,max:Int):Option[List[String]]={
    init
    val solrQuery = new SolrQuery();
    solrQuery.setQueryType("standard");
    // Facets
    solrQuery.setFacet(true)
    solrQuery.addFacetField(field)
    solrQuery.setQuery(query)
    solrQuery.setRows(0)
    solrQuery.setFacetLimit(max)
    solrQuery.setFacetMinCount(1)
    val response = solrServer.query(solrQuery)
    val facets = response.getFacetField(field)
    //TODO page through the facets to make more efficient.
    val values = facets.getValues().asScala[org.apache.solr.client.solrj.response.FacetField.Count]
    if (values.size > 0) {
      Some(values.map(facet => facet.getName).asInstanceOf[List[String]]);
    }
    else
      None
  }
  
    /**
     * Writes the list of row_keys for the results of the specified query to the
     * output stream.
     */
    override def writeRowKeysToStream(query:String, outputStream: OutputStream){
        init
        val size =100;
        var start =0;
        val solrQuery = new SolrQuery();
        var continue = true
        solrQuery.setQueryType("standard");
        solrQuery.setFacet(false)
        solrQuery.setFields("row_key")        
        solrQuery.setQuery(query)
        solrQuery.setRows(100)
        while(continue){
            solrQuery.setStart(start)
            val response = solrServer.query(solrQuery)
            
            val resultsIterator =response.getResults().iterator
            while(resultsIterator.hasNext){
                val result = resultsIterator.next()
                outputStream.write((result.getFieldValue("row_key")+ "\n").getBytes())
            }
            
            start +=size
            continue = response.getResults.getNumFound > start
        }
    }
    

    def printNumDocumentsInIndex():String = {
        val rq = solrServer.query(new SolrQuery("*:*"))
        ">>>>>>>>>>>>>Document count of index: " + rq.getResults().getNumFound()
    }

   class AddDocThread(queue: ArrayBlockingQueue[java.util.List[SolrInputDocument]], id:Int) extends Thread {

        private var shouldRun = true;
        
        def stopRunning = { shouldRun = false }

        override def run(){
            println("Starting AddDocThread thread....")
            while(shouldRun || queue.size >0){
                if(queue.size > 0){
                    var docs = queue.poll()
                    //add and commit the docs
                    if (docs != null && !docs.isEmpty) {
                        try{
                          logger.info("Thread " + id + " is adding " + docs.size + " documents to the index.")
                          solrServer.add(docs)
                          //only the first thread should commit
                          if(id == 0) solrServer.commit
                          docs = null
                        }
                        catch {
                            case _=> //do nothing
                        }
                    }
                }
                else {
                    try {
                      Thread.sleep(250);
                    }
                    catch {
                        case _ => //do nothing
                    }
                }
            }
            println("Finishing AddDocThread thread.")
        }
    }    

  /**
   * The Actor which allows solr index inserts to be performed on a Thread.
   * solrServer.add(docs) is not thread safe - we should only ever have one thread adding documents to the solr server
   */
//  class SolrIndexActor extends Actor{
//    var processed = 0
//    var received =0
//
//    def ready = processed==received
//    def act {
//      loop{
//        react{
//          case docs:java.util.ArrayList[SolrInputDocument] =>{
//            //send them off to the solr server using a thread...
//            println("Sending docs to SOLR "+ docs.size+"-" + received)
//            received += 1
//            try {
//                solrServer.add(docs)
//                solrServer.commit
//                //printNumDocumentsInIndex
//            } catch {
//                case e: Exception => logger.error(e.getMessage, e)
//            }
//            processed +=1
//          }
//          case msg:String =>{
//              if(msg == "reload"){
//                cc.reload("")
//              }
//              if(msg == "exit")
//                exit()
//          }
//        }
//      }
//    }
//  }
}