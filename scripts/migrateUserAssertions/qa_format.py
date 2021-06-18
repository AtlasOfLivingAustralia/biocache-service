import json
import re

def normaluid(uuid):
    return re.search("^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$", uuid) != None
    #return ("|" not in uuid) and (not uuid.startswith("dr"))

def load_rowkey_mapping(mappings):
    mappingFile = r'./avh-orphaned-annotations.txt'
    with open(mappingFile) as f:
        for row in f:
            x = row.strip().replace('\t', ' ').split(' ')
            if len(x) <= 1:
                None
            else:
                mappings[x[0]] = x[-1]
    print('mapping size = %d ' % len(mappings))

def load_uuid_mapping(mapping):
    mappingFile = r'./uuid_mapping_niels.txt'
    with open(mappingFile) as f:
        for row in f:
            if not row.startswith("#"):
                x = row.strip().split(' ')
                if len(x) >= 2 and normaluid(x[0]) and normaluid(x[-1]):
                    mapping[x[0]] = x[-1]

def convert(csvFilePath, jsonFilePath):
    data = {}
    rowkey_to_uuid = {}
    load_rowkey_mapping(rowkey_to_uuid)
    uuid_mapping = {}
    load_uuid_mapping(uuid_mapping)

    # all the uuids rowkeys map to
    newUUIDs = set(rowkey_to_uuid.values())

    # check if a target uuid already in rowkey_mappings
    keys = [uuid for uuid in uuid_mapping.keys() if uuid_mapping[uuid] in newUUIDs]
    for uuid in keys:
        del uuid_mapping[uuid]

    with open(csvFilePath, encoding='utf-8') as csvf:
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

        with open(jsonFilePath, 'w', encoding='utf-8') as csvf:
            for k in data:
                temp = json.dumps(data[k], indent=False).replace('\n', '')
                if (k in rowkey_to_uuid) and (not rowkey_to_uuid[k] in data):
                    print("find mapping for " + k + ' -> ' + rowkey_to_uuid[k])
                    k = rowkey_to_uuid[k]
                elif k in uuid_mapping:
                    print("find mapping for " + k + ' -> ' + uuid_mapping[k])
                    k = uuid_mapping[k]
                if normaluid(k):
                    # print("write key = " + k)
                    csvf.write('\t'.join([k, temp]) + '\n')

csvFilePath = r'/data/tmp/qa_dump.cql'
jsonFilePath = r'/data/tmp/qa.csv'
convert(csvFilePath, jsonFilePath)
