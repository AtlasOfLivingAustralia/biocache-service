<%@ include file="/common/taglibs.jsp"%><%
        /*
         * JSON representation of occurrence points at a location for a given occurrence search
         *
         * Document   : infoPointGeojson
         * Created on : Feb 21, 2011, 4:51:43 PM
         * @author "Ajay Ranipeta <Ajay.Ranipeta@csiro.au>"
         */
%><%@ page contentType="application/json; charset=UTF-8" %>
<c:if test="${not empty param['callback']}">${param['callback']}(</c:if>
<json:object prettyPrint="false">
    <json:property name="count" value="${count}"/>
    <json:array name="occurrences" var="point" items="${points}">
        <json:property name="ouid" value="${point.occurrenceUid}"/>
    </json:array>
</json:object>
<c:if test="${not empty param['callback']}">)</c:if>
