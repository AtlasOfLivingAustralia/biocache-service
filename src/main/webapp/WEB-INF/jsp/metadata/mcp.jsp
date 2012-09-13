<%@page contentType="text/xml" %><%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"
%><?xml version="1.0" encoding="UTF-8" ?>
<mcp:MD_Metadata xmlns:mcp="http://bluenet3.antcrc.utas.edu.au/mcp"
                 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                 xmlns:gml="http://www.opengis.net/gml"
                 xmlns:gts="http://www.isotc211.org/2005/gts"
                 xmlns:gco="http://www.isotc211.org/2005/gco"
                 xmlns:gmd="http://www.isotc211.org/2005/gmd"
                 xmlns:geonet="http://www.fao.org/geonetwork"
                 gco:isoType="gmd:MD_Metadata"
                 xsi:schemaLocation="&#xA;&#x9;&#x9;&#x9;&#x9;http://www.isotc211.org/2005/gmd/ http://www.isotc211.org/2005/gmd/gmd.xsd http://www.isotc211.org/2005/srv/ http://schemas.opengis.net/iso/19139/20060504/srv/srv.xsd http://bluenet3.antcrc.utas.edu.au/mcp/ http://bluenet3.antcrc.utas.edu.au/mcp/schema.xsd&#xA;&#x9;&#x9;&#x9;">
  <gmd:fileIdentifier>
      <gco:CharacterString xmlns:srv="http://www.isotc211.org/2005/srv"
                           xmlns:gmx="http://www.isotc211.org/2005/gmx"
                           xmlns:xlink="http://www.w3.org/1999/xlink">${guid}</gco:CharacterString>
  </gmd:fileIdentifier>
  <gmd:language>
      <gco:CharacterString xmlns:srv="http://www.isotc211.org/2005/srv">eng</gco:CharacterString>
  </gmd:language>
  <gmd:characterSet>
      <gmd:MD_CharacterSetCode xmlns:srv="http://www.isotc211.org/2005/srv" codeListValue="utf8"
                               codeList="http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#MD_CharacterSetCode">utf8</gmd:MD_CharacterSetCode>
  </gmd:characterSet>
  <gmd:parentIdentifier>
      <gco:CharacterString>${name} ${authorship}</gco:CharacterString>
  </gmd:parentIdentifier>
  <gmd:hierarchyLevel>
      <gmd:MD_ScopeCode codeList="http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#MD_ScopeCode"
                        codeListValue="series">series</gmd:MD_ScopeCode>
  </gmd:hierarchyLevel>
  <gmd:hierarchyLevelName>
      <gco:CharacterString>Atlas of Living Australia Species Occurrence Layer</gco:CharacterString>
  </gmd:hierarchyLevelName>
  <gmd:contact>
      <gmd:CI_ResponsibleParty>
         <gmd:organisationName>
            <gco:CharacterString>Atlas of Living Australia</gco:CharacterString>
         </gmd:organisationName>
         <gmd:contactInfo>
            <gmd:CI_Contact>
               <gmd:phone>
                  <gmd:CI_Telephone>
                     <gmd:voice>
                        <gco:CharacterString>61 2 6246 4431</gco:CharacterString>
                     </gmd:voice>
                  </gmd:CI_Telephone>
               </gmd:phone>
               <gmd:address>
                  <gmd:CI_Address>
                     <gmd:deliveryPoint>
                        <gco:CharacterString>GPO Box 1700</gco:CharacterString>
                     </gmd:deliveryPoint>
                     <gmd:city>
                        <gco:CharacterString>Canberra</gco:CharacterString>
                     </gmd:city>
                     <gmd:administrativeArea>
                        <gco:CharacterString>Acton</gco:CharacterString>
                     </gmd:administrativeArea>
                     <gmd:postalCode>
                        <gco:CharacterString>2601</gco:CharacterString>
                     </gmd:postalCode>
                     <gmd:country>
                        <gco:CharacterString>Australia</gco:CharacterString>
                     </gmd:country>
                     <gmd:electronicMailAddress>
                        <gco:CharacterString>info@ala.org.au</gco:CharacterString>
                     </gmd:electronicMailAddress>
                  </gmd:CI_Address>
               </gmd:address>
               <gmd:onlineResource>
                  <gmd:CI_OnlineResource>
                     <gmd:linkage>
                        <gmd:URL>http://www.ala.org.au</gmd:URL>
                     </gmd:linkage>
                     <gmd:protocol>
                        <gco:CharacterString>WWW:LINK-1.0-http--link</gco:CharacterString>
                     </gmd:protocol>
                     <gmd:name gco:nilReason="missing">
                        <gco:CharacterString/>
                     </gmd:name>
                     <gmd:description>
                        <gco:CharacterString>Atlas of Living Australia</gco:CharacterString>
                     </gmd:description>
                     <gmd:function>
                        <gmd:CI_OnLineFunctionCode codeList="http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#CI_OnLineFunctionCode"
                                                   codeListValue="information">information</gmd:CI_OnLineFunctionCode>
                     </gmd:function>
                  </gmd:CI_OnlineResource>
               </gmd:onlineResource>
               <gmd:hoursOfService>
                  <gco:CharacterString>9am to 5pm UTC+10: Monday to Friday</gco:CharacterString>
               </gmd:hoursOfService>
            </gmd:CI_Contact>
         </gmd:contactInfo>
         <gmd:role>
            <gmd:CI_RoleCode codeList="http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#CI_RoleCode"
                             codeListValue="distributor">distributor</gmd:CI_RoleCode>
         </gmd:role>
      </gmd:CI_ResponsibleParty>
  </gmd:contact>
  <gmd:dateStamp xmlns:srv="http://www.isotc211.org/2005/srv"
                  xmlns:gmx="http://www.isotc211.org/2005/gmx"
                  xmlns:xlink="http://www.w3.org/1999/xlink">
      <gco:DateTime>2012-01-24T13:52:25</gco:DateTime>
  </gmd:dateStamp>
  <gmd:metadataStandardName>
      <gco:CharacterString xmlns:srv="http://www.isotc211.org/2005/srv"
                           xmlns:gmx="http://www.isotc211.org/2005/gmx"
                           xmlns:xlink="http://www.w3.org/1999/xlink">Australian Marine Community Profile of ISO 19115:2005/19139</gco:CharacterString>
  </gmd:metadataStandardName>
  <gmd:metadataStandardVersion>
      <gco:CharacterString xmlns:srv="http://www.isotc211.org/2005/srv"
                           xmlns:gmx="http://www.isotc211.org/2005/gmx"
                           xmlns:xlink="http://www.w3.org/1999/xlink">MCP:BlueNet V1.4</gco:CharacterString>
  </gmd:metadataStandardVersion>


  <gmd:identificationInfo>
      <mcp:MD_DataIdentification gco:isoType="gmd:MD_DataIdentification">
         <gmd:citation>
            <gmd:CI_Citation>
               <gmd:title>
                  <gco:CharacterString>ALA Species Occurrence records</gco:CharacterString>
               </gmd:title>
               <gmd:date>
                  <gmd:CI_Date>
                     <gmd:date>
                        <gco:Date>2012-01-24</gco:Date>
                     </gmd:date>
                     <gmd:dateType>
                        <gmd:CI_DateTypeCode codeList="http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#CI_DateTypeCode"
                                             codeListValue="creation">creation</gmd:CI_DateTypeCode>
                     </gmd:dateType>
                  </gmd:CI_Date>
               </gmd:date>
               <gmd:citedResponsibleParty>
                  <gmd:CI_ResponsibleParty>
                     <gmd:organisationName>
                        <gco:CharacterString>Atlas of Living Australia</gco:CharacterString>
                     </gmd:organisationName>
                     <gmd:contactInfo>
                        <gmd:CI_Contact>
                           <gmd:phone>
                              <gmd:CI_Telephone>
                                 <gmd:voice>
                                    <gco:CharacterString>61 2 6246 4439</gco:CharacterString>
                                 </gmd:voice>
                                 <gmd:facsimile>
                                    <gco:CharacterString>61 2 6246 4000</gco:CharacterString>
                                 </gmd:facsimile>
                              </gmd:CI_Telephone>
                           </gmd:phone>
                           <gmd:address>
                              <gmd:CI_Address>
                                 <gmd:deliveryPoint>
                                    <gco:CharacterString>GPO Box 1700</gco:CharacterString>
                                 </gmd:deliveryPoint>
                                 <gmd:city>
                                    <gco:CharacterString>Acton</gco:CharacterString>
                                 </gmd:city>
                                 <gmd:administrativeArea>
                                    <gco:CharacterString>Canberra</gco:CharacterString>
                                 </gmd:administrativeArea>
                                 <gmd:postalCode>
                                    <gco:CharacterString>2601</gco:CharacterString>
                                 </gmd:postalCode>
                                 <gmd:country>
                                    <gco:CharacterString>Australia</gco:CharacterString>
                                 </gmd:country>
                                 <gmd:electronicMailAddress>
                                    <gco:CharacterString>support@alaorg.au</gco:CharacterString>
                                 </gmd:electronicMailAddress>
                              </gmd:CI_Address>
                           </gmd:address>
                           <gmd:onlineResource>
                              <gmd:CI_OnlineResource>
                                 <gmd:linkage>
                                    <gmd:URL>http://www.ala.org.au/</gmd:URL>
                                 </gmd:linkage>
                                 <gmd:protocol>
                                    <gco:CharacterString>WWW:LINK-1.0-http--link</gco:CharacterString>
                                 </gmd:protocol>
                                 <gmd:name>
                                    <gco:CharacterString>Website of the Atlas of Living Australia</gco:CharacterString>
                                 </gmd:name>
                                 <gmd:description gco:nilReason="missing">
                                    <gco:CharacterString/>
                                 </gmd:description>
                              </gmd:CI_OnlineResource>
                           </gmd:onlineResource>
                           <gmd:hoursOfService>
                              <gco:CharacterString>9am to 5pm UTC+10: Monday to Friday</gco:CharacterString>
                           </gmd:hoursOfService>
                        </gmd:CI_Contact>
                     </gmd:contactInfo>
                     <gmd:role>
                        <gmd:CI_RoleCode codeList="http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#CI_RoleCode"
                                         codeListValue="resourceProvider">resourceProvider</gmd:CI_RoleCode>
                     </gmd:role>
                  </gmd:CI_ResponsibleParty>
               </gmd:citedResponsibleParty>
               <gmd:otherCitationDetails>
                  <gco:CharacterString>The citation in a list of references is: "ALA [year-of-data-downloaded], [Title], [data-access-url], accessed [date-of-access]"</gco:CharacterString>
               </gmd:otherCitationDetails>
            </gmd:CI_Citation>
         </gmd:citation>
         <gmd:abstract>
            <gco:CharacterString>
            <![CDATA[

                ${name} ${authorship}<br/>
                ${commonName}<br/>

                <c:if test="${not empty dataProviders}">
                <br/>
                Data providers:<br/>

                <c:forEach items="${dataProviders}" var="facet">
                ${facet.count} records from ${facet.label} <br/>
                </c:forEach>

                </c:if>

                <c:if test="${not empty imageUrl}">
                    <img src="${imageUrl}" alt="Representative image of ${name}"/>
                    <span> Representative image of <a href="${speciesPageUrl}">${name}<c:if test="${not empty commonName}">:</c:if>${commonName}</a> <span>
                    <c:if test="${not empty imageCreator}"><br/>Image creator: ${imageCreator}</c:if>
                    <c:if test="${not empty imageSource}"><br/>Image source: ${imageSource}</c:if>
                    <c:if test="${not empty imageLicence}"><br/>Image licence: ${imageLicence}</c:if>
                </c:if>

                <c:if test="${empty imageUrl}">
                    <a href="${speciesPageUrl}">View more information for ${name}
                        <c:if test="${not empty commonName}">:</c:if>
                        ${commonName}</a>
                </c:if>
            ]]>
            </gco:CharacterString>
            </gmd:abstract>
         <gmd:credit>
            <gco:CharacterString>

            	Atlas of Living Australia. ALA is supported by the Australian Government through the National
            	Collaborative Research Infrastructure Strategy (NCRIS) and the Super Science Initiative (SSI).

            </gco:CharacterString>
         </gmd:credit>
         <gmd:credit>
            <gco:CharacterString>

            	Funded by the National Collaborative Research Infrastructure Strategy (NCRIS) and the Super Science Initiative (SSI).

            </gco:CharacterString>
         </gmd:credit>
         <gmd:status>
            <gmd:MD_ProgressCode codeList="http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#MD_ProgressCode"
                                 codeListValue="onGoing">onGoing</gmd:MD_ProgressCode>
         </gmd:status>
         <gmd:pointOfContact>
            <gmd:CI_ResponsibleParty>
               <gmd:individualName>
                  <gco:CharacterString>ALA Support</gco:CharacterString>
               </gmd:individualName>
               <gmd:organisationName>
                  <gco:CharacterString>Atlas of Living Australia</gco:CharacterString>
               </gmd:organisationName>
               <gmd:contactInfo>
                  <gmd:CI_Contact>
                     <gmd:phone>
                        <gmd:CI_Telephone/>
                     </gmd:phone>
                     <gmd:address>
                        <gmd:CI_Address>
                           <gmd:electronicMailAddress>
                              <gco:CharacterString>support@ala.org.au</gco:CharacterString>
                           </gmd:electronicMailAddress>
                        </gmd:CI_Address>
                     </gmd:address>
                  </gmd:CI_Contact>
               </gmd:contactInfo>
               <gmd:role>
                  <gmd:CI_RoleCode codeList="http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#CI_RoleCode"
                                   codeListValue="pointOfContact">pointOfContact</gmd:CI_RoleCode>
               </gmd:role>
            </gmd:CI_ResponsibleParty>
         </gmd:pointOfContact>
         <gmd:resourceMaintenance>
            <gmd:MD_MaintenanceInformation>
               <gmd:maintenanceAndUpdateFrequency>
                  <gmd:MD_MaintenanceFrequencyCode codeList="http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#MD_MaintenanceFrequencyCode"
                                                   codeListValue="irregular">irregular</gmd:MD_MaintenanceFrequencyCode>
               </gmd:maintenanceAndUpdateFrequency>
            </gmd:MD_MaintenanceInformation>
         </gmd:resourceMaintenance>
         <gmd:graphicOverview>
            <gmd:MD_BrowseGraphic>
               <gmd:fileName>
                  <gco:CharacterString>NRS_thumb_s.png</gco:CharacterString>
               </gmd:fileName>
               <gmd:fileDescription>
                  <gco:CharacterString>thumbnail</gco:CharacterString>
               </gmd:fileDescription>
               <gmd:fileType>
                  <gco:CharacterString>png</gco:CharacterString>
               </gmd:fileType>
            </gmd:MD_BrowseGraphic>
         </gmd:graphicOverview>
         <gmd:graphicOverview>
            <gmd:MD_BrowseGraphic>
               <gmd:fileName>
                  <gco:CharacterString>NRS_thumb.png</gco:CharacterString>
               </gmd:fileName>
               <gmd:fileDescription>
                  <gco:CharacterString>large_thumbnail</gco:CharacterString>
               </gmd:fileDescription>
               <gmd:fileType>
                  <gco:CharacterString>png</gco:CharacterString>
               </gmd:fileType>
            </gmd:MD_BrowseGraphic>
         </gmd:graphicOverview>
         <gmd:descriptiveKeywords>
            <gmd:MD_Keywords>
               <gmd:keyword>
                  <gco:CharacterString>Oceans | Salinity/density | Conductivity</gco:CharacterString>
               </gmd:keyword>

               <gmd:type>
                  <gmd:MD_KeywordTypeCode codeList="http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#MD_KeywordTypeCode"
                                          codeListValue="theme">theme</gmd:MD_KeywordTypeCode>
               </gmd:type>
               <gmd:thesaurusName>
                  <gmd:CI_Citation>
                     <gmd:title>
                        <gco:CharacterString>NASA/Global Change Master Directory Earth Science Keywords Version 5.3.8</gco:CharacterString>
                     </gmd:title>
                     <gmd:alternateTitle>
                        <gco:CharacterString>GCMD</gco:CharacterString>
                     </gmd:alternateTitle>
                     <gmd:date>
                        <gmd:CI_Date>
                           <gmd:date>
                              <gco:Date>2006-01-01</gco:Date>
                           </gmd:date>
                           <gmd:dateType>
                              <gmd:CI_DateTypeCode codeList="http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#CI_DateTypeCode"
                                                   codeListValue="revision">revision</gmd:CI_DateTypeCode>
                           </gmd:dateType>
                        </gmd:CI_Date>
                     </gmd:date>
                     <gmd:citedResponsibleParty>
                        <gmd:CI_ResponsibleParty>
                           <gmd:organisationName>
                              <gco:CharacterString>National Aeronautics and Space Administration (NASA)</gco:CharacterString>
                           </gmd:organisationName>
                           <gmd:role>
                              <gmd:CI_RoleCode codeList="http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#CI_RoleCode"
                                               codeListValue="owner">owner</gmd:CI_RoleCode>
                           </gmd:role>
                        </gmd:CI_ResponsibleParty>
                     </gmd:citedResponsibleParty>
                  </gmd:CI_Citation>
               </gmd:thesaurusName>
            </gmd:MD_Keywords>
         </gmd:descriptiveKeywords>

         <gmd:resourceConstraints>
            <gmd:MD_Constraints>
               <gmd:useLimitation>
                  <gco:CharacterString>
                  Any users of ALA data are required to clearly acknowledge the source of the material in the format:
                  "Data was sourced from the Atlas of Living Australia (ALA). ALA is supported by the Australian Government through
                   the National Collaborative Research Infrastructure Strategy and the Super Science Initiative. "
                   </gco:CharacterString>
               </gmd:useLimitation>
               <gmd:useLimitation>
                  <gco:CharacterString>
                  The citation in a list of references is: "ALA [year-of-data-downloaded], [Title], [data-access-url}, accessed [date-of-access]"</gco:CharacterString>
               </gmd:useLimitation>
               <gmd:useLimitation>
                  <gco:CharacterString>Data, products and services from ALA are provided "as is" without any warranty as to fitness for a particular purpose.</gco:CharacterString>
               </gmd:useLimitation>
            </gmd:MD_Constraints>
         </gmd:resourceConstraints>
         <gmd:resourceConstraints>
            <gmd:MD_SecurityConstraints>
               <gmd:classification>
                  <gmd:MD_ClassificationCode codeList="http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#MD_ClassificationCode"
                                             codeListValue="unclassified">unclassified</gmd:MD_ClassificationCode>
               </gmd:classification>
            </gmd:MD_SecurityConstraints>
         </gmd:resourceConstraints>
         <gmd:language>
            <gco:CharacterString xmlns:srv="http://www.isotc211.org/2005/srv">eng</gco:CharacterString>
         </gmd:language>
         <gmd:characterSet>
            <gmd:MD_CharacterSetCode codeList="http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#MD_CharacterSetCode"
                                     codeListValue="utf8">utf8</gmd:MD_CharacterSetCode>
         </gmd:characterSet>
         <gmd:topicCategory>
            <gmd:MD_TopicCategoryCode>oceans</gmd:MD_TopicCategoryCode>
         </gmd:topicCategory>
         <gmd:extent>
            <gmd:EX_Extent>
               <gmd:geographicElement>
                  <gmd:EX_GeographicBoundingBox>
                     <gmd:westBoundLongitude>
                        <gco:Decimal>112</gco:Decimal>
                     </gmd:westBoundLongitude>
                     <gmd:eastBoundLongitude>
                        <gco:Decimal>154</gco:Decimal>
                     </gmd:eastBoundLongitude>
                     <gmd:southBoundLatitude>
                        <gco:Decimal>-44</gco:Decimal>
                     </gmd:southBoundLatitude>
                     <gmd:northBoundLatitude>
                        <gco:Decimal>-9</gco:Decimal>
                     </gmd:northBoundLatitude>
                  </gmd:EX_GeographicBoundingBox>
               </gmd:geographicElement>
            </gmd:EX_Extent>
         </gmd:extent>
      </mcp:MD_DataIdentification>
  </gmd:identificationInfo>
  <gmd:distributionInfo xmlns:srv="http://www.isotc211.org/2005/srv">
      <gmd:MD_Distribution>
         <gmd:distributionFormat>
            <gmd:MD_Format>
               <gmd:name gco:nilReason="missing">
                  <gco:CharacterString/>
               </gmd:name>
               <gmd:version gco:nilReason="missing">
                  <gco:CharacterString/>
               </gmd:version>
            </gmd:MD_Format>
         </gmd:distributionFormat>
         <gmd:transferOptions>
            <gmd:MD_DigitalTransferOptions>
               <gmd:onLine>
                  <gmd:CI_OnlineResource>
                     <gmd:linkage xmlns:gmx="http://www.isotc211.org/2005/gmx"
                                  xmlns:xlink="http://www.w3.org/1999/xlink">
                        <gmd:URL>http://biocache.ala.org.au/occurrences/search?q=${query}</gmd:URL>
                     </gmd:linkage>
                     <gmd:protocol xmlns:gmx="http://www.isotc211.org/2005/gmx"
                                   xmlns:xlink="http://www.w3.org/1999/xlink">
                        <gco:CharacterString>WWW:LINK-1.0-http--metadata-URL</gco:CharacterString>
                     </gmd:protocol>
                     <gmd:description xmlns:gmx="http://www.isotc211.org/2005/gmx"
                                      xmlns:xlink="http://www.w3.org/1999/xlink">
                        <gco:CharacterString>View all these records in a table view</gco:CharacterString>
                     </gmd:description>
                  </gmd:CI_OnlineResource>
               </gmd:onLine>
            </gmd:MD_DigitalTransferOptions>
         </gmd:transferOptions>
      </gmd:MD_Distribution>
  </gmd:distributionInfo>
  <gmd:dataQualityInfo>
      <gmd:DQ_DataQuality>
         <gmd:scope>
            <gmd:DQ_Scope>
               <gmd:level>
                  <gmd:MD_ScopeCode codeList="http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#MD_ScopeCode" codeListValue=""/>
               </gmd:level>
               <gmd:levelDescription>
                  <gmd:MD_ScopeDescription>
                     <gmd:other>
                        <gco:CharacterString>ALA occurrence records</gco:CharacterString>
                     </gmd:other>
                  </gmd:MD_ScopeDescription>
               </gmd:levelDescription>
            </gmd:DQ_Scope>
         </gmd:scope>
      </gmd:DQ_DataQuality>
  </gmd:dataQualityInfo>
  <mcp:revisionDate>
      <gco:DateTime xmlns:srv="http://www.isotc211.org/2005/srv"
                    xmlns:gmx="http://www.isotc211.org/2005/gmx"
                    xmlns:xlink="http://www.w3.org/1999/xlink">2012-07-23T12:15:17</gco:DateTime>
  </mcp:revisionDate>
</mcp:MD_Metadata>
