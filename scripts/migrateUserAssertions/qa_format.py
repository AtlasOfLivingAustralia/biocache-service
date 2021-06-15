import json

def loadmapping(filename, mappings):
    with open(filename) as f:
        for row in f:
            x = row.strip().replace('\t', ' ').split(' ')
            if (len(x) == 1):
                None
            else:
                mappings[x[0]] = x[-1]
    print('mapping size = ' + str(len(mappings)))

def convert(csvFilePath, jsonFilePath, mappingFile):
    data = {}
    uuidmapping = {}
    loadmapping(mappingFile, uuidmapping)

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
                if (k in uuidmapping) and (not uuidmapping[k] in data):
                    print("find mapping for " + k + ' -> ' + uuidmapping[k])
                    k = uuidmapping[k]
                csvf.write('\t'.join([k, temp]) + '\n')

csvFilePath = r'/data/tmp/qa_dump.cql'
jsonFilePath = r'/data/tmp/qa.csv'
mappingFile = r'./avh-orphaned-annotations.txt'
convert(csvFilePath, jsonFilePath, mappingFile)
