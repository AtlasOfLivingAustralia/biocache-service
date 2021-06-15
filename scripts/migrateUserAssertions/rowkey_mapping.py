def normaluid(uuid):
    return ("|" not in uuid) and (not uuid.startswith("dr"))

def process(inputFilePath):
    data = {}
    assertion_map = {}

    mapped_map = {}
    unmapped_set = set()

    with open(inputFilePath, encoding='utf-8') as csvf:
        delimiter = '\t'
        linenum = 0
        columns = {}
        for row in csvf:
            contents = row.rstrip('\n').split(delimiter)
            # first line is header
            if linenum == 0:
                linenum = linenum + 1
                for idx in range(0, len(contents)):
                    columns[idx] = contents[idx]
            else:
                if len(contents) >= len(columns):
                    oneline = {}
                    for idx in range(0, len(contents)):
                        # cqlsh COPY TO sometimes wrap values with ""
                        if contents[idx].startswith('"') and contents[idx].endswith('"'):
                            contents[idx] = contents[idx][1:-1]
                        oneline[columns[idx]] = contents[idx]

                    key = oneline['rowkey']
                    if key not in data:
                        data[key] = []

                    data[key].append(oneline)

    print("total record size = %d" % (len(data)))
    normals = [key for key in data if normaluid(key)]
    abnormals = [key for key in data if not normaluid(key)]
    dr376s = [key for key in abnormals if key.startswith("dr376")]

    print("%d records referenced by uuid\n%d records referenced by rowkey, %d belong to dr376" % (len(normals), len(abnormals), len(dr376s)))
    if len(data) != len(normals) + len(abnormals):
        print("normal.count + abnormal.count != len(data) *****************")


    with open('/data/log.txt', 'w') as log:
        processed = 0
        for k in data:
            if normaluid(k):
                processed += 1
                for assertion in data[k]:
                    uniquekey = getHash(assertion)
                    assertion_map[uniquekey] = k
        print("%d uuid records processed" % processed)

        processed = 0
        for k in data:
            log.write("record id [" + k + "]\n")
            i = 0
            logged = False
            for assertion in data[k]:
                log.write("   assertion[" + str(i) +"].uuid = " + assertion['uuid'] + "\n")
                log.write("   assertion[" + str(i) +"].name = " + assertion['name'] + "\n")
                log.write("   assertion[" + str(i) +"].code = " + assertion['code'] + "\n")
                log.write("   assertion[" + str(i) +"].comment = " + assertion['comment'] + "\n")
                log.write("   assertion[" + str(i) +"].userDisplayName = " + assertion['userDisplayName'] + "\n")
                log.write("   assertion[" + str(i) +"].referenceRowKey = " + assertion['referenceRowKey'] + "\n")
                log.write("   assertion[" + str(i) +"].created = " + assertion['created'] + "\n")
                i = i + 1

                if not normaluid(k):
                    if not logged:
                        logged = True
                        processed += 1

                    # this should be unique
                    localuniquekey = getHash(assertion)
                    if localuniquekey in assertion_map:
                        log.write("  this abnormal assertion already exists !!!! : " + k + " -> " + assertion_map[localuniquekey])
                        mapped_map[k] = assertion_map[localuniquekey]
                    else:
                        log.write("  this needs to be check, this abnormal assertion has no mapping")
                        unmapped_set.add(k)

            log.write("\n")
        print("%d referenceRowKey checked" % processed)

    with open('/data/mapped.txt', 'w') as fmapped, open('/data/un-mapped.txt', 'w') as funmapped:
        print("%d records mapped, %d records NOT mapped\n" % (len(mapped_map), len(unmapped_set)))
        for key in sorted(mapped_map):
            fmapped.write('{0: <80}'.format(key) + " -> " + mapped_map[key] + "\n")

        for key in sorted(unmapped_set):
            funmapped.write(key + "\n")

def getHash(assertion):
    return assertion['name'] + '|' + assertion['code'] + '|' + assertion['userDisplayName'] + '|' + assertion['created']

# user assertions from prod database could be referenced by rowkey instead of uuid
# when we do the index, we need the uuid of the record
# This module is used to find out which rowkey is already mapped to an uuid and which not
if __name__ == "__main__":
    inputFilePath = r'/data/tmp/qa_dump_Jun15.cql'
    process(inputFilePath)
