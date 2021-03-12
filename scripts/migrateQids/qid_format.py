import csv
import json

csv.field_size_limit(2147483647)

def convert(csvFilePath, jsonFilePath):
    data = {}

    with open(csvFilePath, encoding='utf-8') as csvf:
        csvReader = csv.DictReader(csvf, dialect='unix', delimiter='\t')
        for rows in csvReader:
            if len(rows) > 2:
                key = rows['rowkey']
                if key not in data:
                    data[key] = []
                for s in rows:
                    rows[s] = rows[s].replace('\t', ' ')
                data[key].append(rows);

        with open(jsonFilePath, 'w', encoding='utf-8') as csvfile:
            fieldnames = ['key', 'data']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, dialect='unix', delimiter='\t',
                                    quoting=csv.QUOTE_NONE,
                                    doublequote=False, escapechar=None, quotechar=None)
            for k in data:
                map = {}
                map['key'] = k;
                map['data'] = json.dumps(data[k], indent=False).replace('\n', '')
                writer.writerow(map)


csvFilePath = r'/data/tmp/qid_dump.cql'
jsonFilePath = r'/data/tmp/qid.csv'
convert(csvFilePath, jsonFilePath)
