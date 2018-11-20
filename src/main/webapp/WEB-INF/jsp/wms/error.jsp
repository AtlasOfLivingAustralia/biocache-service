<%@ page contentType="text/xml" pageEncoding="UTF-8" %><?xml version="1.0" encoding="UTF-8"?>
<ServiceExceptionReport version="1.3.0" xmlns="http://www.opengis.net/ogc" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/ogc">
    <ServiceException code="${errorType}">${errorDescription}</ServiceException>
</ServiceExceptionReport>