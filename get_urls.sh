#!/bin/bash
echo "Enter the location of the apache log file"
read alog
echo "Enter the location of the target file"
read tgt
echo "Enter the date you wish to process eg 31/Jul/2013"
read date
echo "Enter the prefix for the http requests eg http://localhost:8080/biocache-service"
read prefix

# This script extracts all the "GET" calls to biocache service from the supplied apache log file.

grep $date $alog | grep GET | grep ws | grep search | sed -n 's/\(.[^"]*\)"GET //p' | sed -n 's/\/ws//p'| sed -n 's/\HTTP\(.*\)//p' | sed -n 's;.*;'$prefix'&;p' > $tgt
