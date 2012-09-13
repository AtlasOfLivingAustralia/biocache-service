<%@page contentType="text/html;UTF-8" %><%@
        taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"
%><html>
<head>
    <title>Occurrence record lookup |  Atlas of Living Australia</title>
</head>
<body>

<h2>Occurrence information <c:if test="">(1 of ${totalRecords}></c:if></h2>

<c:choose>

<c:when test="${not empty record}">
<p>
Scientific name: ${record.taxon_name}<br/>
<c:if test="${not empty record.common_name}">Common name: ${record.common_name}<br/></c:if>
Kingdom: ${record.kingdom}<br/>
Family: ${record.family}<br/>
Data provider: ${record.data_provider}<br/>
Longitude: ${record.longitude}, Latitude: ${record.latitude}<br/>
Spatial uncertainty in metres:
    <c:if test="${empty record.coordinate_uncertainty}">Not supplied</c:if>
    ${record.coordinate_uncertainty}<br/>
Occurrence date: <c:if test="${empty record.occurrence_date}">Not supplied</c:if>${record.occurrence_date}<br/>
Basis of record: <c:if test="${empty record.basis_of_record}">Not supplied</c:if>${record.basis_of_record}
</p>

<p>
    <a href="${uriUrl}">View all records at this point</a>
</p>

<p style="background-color: #515863; padding:5px;">
    <a href="http://www.ala.org.au">
        <img src="${initParam.webservicesRoot}/static/images/logo.png" alt="logo"/>
    </a>
</p>
</c:when>
<c:otherwise>
No records at this point.
</c:otherwise>
</c:choose>
</body>
</html>