"""
Helper functions, runnable as a script
"""
from collections import defaultdict
import csv
import json
import sys

from bson.objectid import ObjectId

from cr.db.store import global_settings as settings
from cr.db.store import connect

settings.update({"url": "mongodb://localhost:27017/test_crunch_fitness"})
db = connect(settings)


def get_dataset(dataset_id=None):
    """
    Get a dataset from the Mongo database
    dataset_id:
        None to pick a dataset at random.
        Otherwise, the hex document ID of the dataset
    Return the dataset document.
    Raise IndexError if no matching dataset found.
    """
    if dataset_id is None:
        dataset_query = {}
    else:
        dataset_query = {'_id': ObjectId(dataset_id)}
    return db.datasets.find(dataset_query)[0]


def scan_dataset(dataset_id=None):
    """
    Scan the values in a dataset and count up unique values.
    dataset_id:
        None to pick a dataset at random.
        Otherwise, the hex document ID of the dataset
    Return a dictionary: {header: {col_value: count}}
    """
    dataset = get_dataset(dataset_id)
    headers = dataset['headers']
    columns = dataset['columns']
    result = defaultdict(lambda: defaultdict(int))
    for i, column in enumerate(columns):
        header = headers[i]
        for value in column:
            result[header][value] += 1
    return result


def scan_csv_cols(csv_filename):
    """
    Scan the values in a CSV file with headers and count up unique values.
    Requires the same number of columns in each row and unique header names.
    Check this using scan_csv_rows().

    Return a dictionary: {header: {col_value: count}}
    """
    with open(csv_filename, 'rU') as f:
        csv_reader = csv.reader(f)
        headers = csv_reader.next()
        result = defaultdict(lambda: defaultdict(int))
        for row in csv_reader:
            for i, header in enumerate(headers):
                result[header][row[i]] += 1
    return result


def scan_csv_rows(csv_filename, *row_indexes):
    """
    Scan a CSV file and verify row size and header consistency.
    Optionally pick some specific rows by index and return them for inspection.
    """
    headers_are_unique = None
    shortest_row = None
    longest_row = None
    num_rows = 0
    specific_rows = []
    row_indexes = [int(i) for i in row_indexes]
    with open(csv_filename, 'rU') as f:
        csv_reader = csv.reader(f)
        headers = csv_reader.next()
        headers_are_unique = (sorted(headers) == sorted(set(headers)))
        for row in csv_reader:
            if shortest_row is None or len(row) < shortest_row:
                shortest_row = len(row)
            if longest_row is None or len(row) > longest_row:
                longest_row = len(row)
            num_rows += 1
            if num_rows in row_indexes:
                specific_rows.append(row)
    return {
        'headers_are_unique': headers_are_unique,
        'shortest_row': shortest_row,
        'longest_row': longest_row,
        'num_rows': num_rows,
        'specific_rows': specific_rows,
    }


def list_datasets():
    return [str(d['_id']) for d in db.datasets.find({}, {'_id': True})]


def main():
    func = getattr(sys.modules[__name__], sys.argv[1])
    result = func(*sys.argv[2:])
    if result is not None:
        json.dump(result, sys.stdout, indent=2, sort_keys=True)


if __name__ == '__main__':
    sys.exit(main())
