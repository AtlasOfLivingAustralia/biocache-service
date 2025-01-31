#!/usr/bin/env bash
#
# Script to run regression tests, taken from:
# https://confluence.csiro.au/display/ALASD/Biocache-service+manual+testing
# (steps 1a to 1s)
#
# Usage: ./regression-tests.sh
#
# Note: does not perform the offline download (step 1.o), as it requires an email address

# Colour vars
red=`tput setaf 1`
green=`tput setaf 2`
magenta=`tput setaf 5`
cyan=`tput setaf 6`
gray=`tput setaf 244`
reset=`tput sgr0`

# Base URL
base_url="https://biocache-ws-test.ala.org.au/ws"

# Check required utilities are installed
command_to_check=("http" "jq" "wc")
for cmd in "${command_to_check[@]}"; do
    if ! command -v "$cmd" > /dev/null; then
        echo "$red$cmd$reset is not available on this system, please install $red$cmd$reset first"
        exit 1
    fi
done

check_search_records() {
  local query=$1
  local expected=$2
  local url="${base_url}/occurrences/search?${query}"
  echo -n "Checking $green${query}$reset returns > ${expected} records => " \
    && http "${url}" | jq -e ".totalRecords > ${expected}"
}

check_csv_line_count() {
  local url=$1
  local expected_count=$2
  local count=$(http "${base_url}${url}" | wc -l )
  if [[ "$count" -le $expected_count ]]; then
    echo "Error: Count is $count, which is not greater than $expected_count" >&2  # Output error to stderr
    exit 1  # Indicate failure with a non-zero exit code
  fi
  echo "Checking ${green}${url}${reset} line count > $expected_count => ${gray}true${reset}"
}

check_jq_expression() {
  local query=$1
  local jq_expr=$2
  local message=$3
  echo -n "Checking ${green}${query}${reset} $message => " && \
    http "${base_url}${query}" | jq -e "${jq_expr}" || exit 1
}

check_search_records "q=acacia+dealbata" 30000
check_search_records "q=taxon_name%3A%22Acacia+dealbata%22" 20000
check_search_records "q=acacia+dealbata&fq=state:\"Australian+Capital+Territory\"" 1000
check_search_records "q=*:*" 150000000 # 155289807
check_search_records "lft:[*+TO+*]" 150000000 # 155289807
check_jq_expression "/occurrence/compare/655027c5-614e-41b8-84df-43c3b95e9c06" '[to_entries[] | .value[] | select(.raw != "") | .raw] | length > 20' "has > 20 \"raw\" values"
check_jq_expression "/occurrence/compare/655027c5-614e-41b8-84df-43c3b95e9c06" '[to_entries[] | .value[] | select(.processed != "") | .processed] | length > 45' "has > 45 \"processed\" values"
check_jq_expression "/index/fields" 'length > 630' "index/fields has > 630 entries"
check_csv_line_count "/index/fields.csv" 630
check_jq_expression "/explore/groups?lat=-35.2649&lon=149.2845&radius=1" '.[] | select(.name == "ALL_SPECIES") | .count > 10000' "has > 10000 \"ALL_SPECIES\" count"
check_jq_expression "/explore/groups?lat=-35.2649&lon=149.2845&radius=1" '.[] | select(.name == "ALL_SPECIES") | .count > 300' "has > 300 \"ALL_SPECIES\" speciesCount"
check_jq_expression "/explore/counts/group/Animals?lat=-35.2649&lon=149.2845&radius=1" '.[0] > 10000' "has > 10000 count"
check_jq_expression "/explore/counts/group/Animals?lat=-35.2649&lon=149.2845&radius=1" '.[1] > 200' "has > 200 speciesCount"
check_csv_line_count "/explore/group/Animals/download?lat=-35.2649&lon=149.2845&radius=1" 200
check_jq_expression "/occurrence/facets?q=acacia&lat=-35.2649&lon=149.2845&radius=5&fq=basis_of_record:HumanObservation&facets=common_name" '.[0].fieldResult[0].count > 10' "has .[0].fieldResult[0].count > 10"
check_jq_expression "/occurrence/facets?q=acacia&lat=-35.2649&lon=149.2845&radius=5&fq=basis_of_record:HumanObservation&facets=common_name" '.[0].fieldResult[0] > 20' "has > 20 fieldResult entries"
check_jq_expression "/occurrence/facets?q=acacia&lat=-35.2649&lon=149.2845&radius=5&fq=basis_of_record:HumanObservation&facets=common_name" '.[0].count > 20' "has \"count\" > 20"
check_jq_expression "/occurrence/e9eeb6ff-2c6f-4f45-b463-ac496bc3bf7c" '.raw | length > 15' "has .raw length > 15"
check_jq_expression "/occurrence/e9eeb6ff-2c6f-4f45-b463-ac496bc3bf7c" '.raw.occurrence | length > 16' "has .raw.occurrence length > 16"
check_jq_expression "/occurrence/e9eeb6ff-2c6f-4f45-b463-ac496bc3bf7c" '.raw.location | length > 16' "has .raw.location length > 16"
check_jq_expression "/occurrence/e9eeb6ff-2c6f-4f45-b463-ac496bc3bf7c" '.processed | length > 15' "has .processed length > 15"
check_jq_expression "/occurrence/e9eeb6ff-2c6f-4f45-b463-ac496bc3bf7c" '.processed.classification | length > 20' "has .processed.classification length > 20"
check_jq_expression "/occurrence/e9eeb6ff-2c6f-4f45-b463-ac496bc3bf7c" '.processed.event | length > 6' "has .processed.event length > 6"
check_jq_expression "/occurrence/e9eeb6ff-2c6f-4f45-b463-ac496bc3bf7c" '.processed.cl | length > 50' "has .processed.cl length > 50"
check_jq_expression "/occurrence/e9eeb6ff-2c6f-4f45-b463-ac496bc3bf7c" '.systemAssertions | length > 3' "has .systemAssertions length > 3"
check_jq_expression "/occurrence/e9eeb6ff-2c6f-4f45-b463-ac496bc3bf7c" '.sensitive == false' "has .sensitive == false"

echo -n "Checking ${green}/occurrences/batchSearch${reset} redirects to 200 HTTP code => " && http --follow --print=h -f POST ${base_url}/occurrences/batchSearch queries="Acacia+dealbata" field="taxa" \
  action="Search" redirectBase="https://biocache-test.ala.org.au/occurrences/search" | head -1 | grep -q 200
if [[ $? -eq 0 ]]; then
  echo "${gray}true${reset}"
else
  echo "${gray}false${reset}"
  exit 1
fi

check_csv_line_count "/occurrences/facets/download?q=taxon_name%3A%22Acacia+dealbata%22&facets=data_resource_uid" 50
check_jq_expression "/occurrences/search?q=text_datasetName:PPBE" '.totalRecords > 25000' "has > 25000 totalRecords"
check_jq_expression "/occurrences/search?q=text_eventID:PPBE" '.totalRecords > 50000' "has > 50000 totalRecords"
check_jq_expression "/occurrences/search?q=text_parentEventID:PPBE" '.totalRecords > 12000' "has > 12000 totalRecords"
check_jq_expression "/occurrences/search?q=text_fieldNumber:PPBE" '.totalRecords > 24000' "has > 24000 totalRecords"