import csv

import sys

csv.field_size_limit(sys.maxsize)


def make_json(csvFilePath, jsonFilePath):
    validKeys = {}

    with open(csvFilePath, encoding='utf-8') as csvf:
        csvReader = csv.reader(csvf)
        for rows in csvReader:
            if len(rows) > 0:
                validKeys[rows[0].strip()] = True

    with open(jsonFilePath, encoding='utf-8') as csvf:
        fieldnames = ['key', 'data']
        csvReader = csv.DictReader(csvf, dialect='unix', delimiter='\t', fieldnames=fieldnames)

        with open(finalFilePath, 'w', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, dialect='unix', delimiter='\t',
                                    quoting=csv.QUOTE_NONE,
                                    doublequote=False, escapechar=None, quotechar=None)
            for rows in csvReader:
                key = rows['key']
                if key in validKeys:
                    writer.writerow(rows)


csvFilePath = r'/data/tmp/valid.rowkeys'
jsonFilePath = r'/data/tmp/qa.csv'
finalFilePath = r'/data/tmp/qa.final.csv'
make_json(csvFilePath, jsonFilePath)
