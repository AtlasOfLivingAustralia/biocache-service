<%@ page contentType="text/html" pageEncoding="UTF-8" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <meta name="pageName" content="home"/>
    <title>Build Info</title>
    <style type="text/css">
        h1 {
            /*color: #48802c;*/
            font-weight: normal;
            font-size: 24px;
            margin: .8em 0 .3em 0;
        }

        table {
            border: 1px solid #ccc;
            font-size: 14px;
        }

        tr {
            border: 0;
        }

        td, th {
            padding: 5px 6px;
            text-align: left;
            vertical-align: top;
        }
    </style>
</head>
<body>

    <h1>Build Info</h1>

    <table>
        <tr>
            <td>Git commit date/time</td><td>${buildInfo.getProperty('git.commit.date')}</td>
        </tr>
        <tr>
            <td>Git commit ID</td><td>${buildInfo.getProperty('git.commit.id')}</td>
        </tr>
        <tr>
            <td>Git commit short ID</td><td>${buildInfo.getProperty('git.commit.shortId')}</td>
        </tr>
        <tr>
            <td>Git branch</td><td>${buildInfo.getProperty('git.branch')}</td>
        </tr>
        <tr>
            <td>Git closest tag name</td><td>${buildInfo.getProperty('git.closest.tag.name')}</td>
        </tr>
        <tr>
            <td>Git closest tag commit count</td><td>${buildInfo.getProperty('git.closest.tag.commit.count')}</td>
        </tr>
    </table>

    <h1>Runtime Application Status</h1>

    <table>
        <tr>
            <td>App version</td><td>${runtimeEnvironment.getProperty('app.version')}</td>
        </tr>
        <tr>
            <td>App build timestamp</td><td>${runtimeEnvironment.getProperty('app.buildTime')}</td>
        </tr>
        <tr>
            <td>Spring version</td><td>${runtimeEnvironment.getProperty('app.spring.version')}</td>
        </tr>
        <tr>
            <td>JVM Version</td><td>${runtimeEnvironment.getProperty('java.version')}</td>
        </tr>
    </table>

</body>
</html>