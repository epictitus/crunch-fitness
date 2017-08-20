"""
Helper functions, runnable as a script
"""
from __future__ import print_function
from collections import defaultdict
import csv
import json
import sys

import bson
from bson.objectid import ObjectId

from cr.db.loader import load_dataset_to_dict
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


def get_dataset_unique_values(dataset_id=None):
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


def scan_dataset(dataset_id=None):
    """
    Scan a dataset, print report of data character counts to stdout
    The objective is to look for low-hanging fruit opportunities to further
    compress the data by normalizing more category strings into integers.
    """
    value_count_map = get_dataset_unique_values(dataset_id)
    char_count_by_header = {}  # { header: char_count }
    for header in sorted(value_count_map):
        char_count = 0
        for value in sorted(value_count_map[header]):
            value_count = value_count_map[header][value]
            if isinstance(value, basestring):
                char_count += value_count * len(value)
        if char_count > 0:
            char_count_by_header[header] = char_count

    headers_by_char_count = defaultdict(list)  # { char_count: [ header, ... ] }
    for header, char_count in char_count_by_header.iteritems():
        headers_by_char_count[char_count].append(header)
    for char_count in sorted(headers_by_char_count, reverse=True):
        print("{} chars: {}".format(char_count, headers_by_char_count[char_count]))


def calc_dataset_size(csv_filename):
    """
    Given a CSV file, estimate the Mongo document size using the bson
    module. This is to see if we will fit under the 16MB Mongo limit.
    """
    data = load_dataset_to_dict(csv_filename)
    b = bson.BSON(data)
    return len(b)


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


def gen_lang_bitmap(csv_filename):
    """
    Scan the values in the WantWorkLanguage column of the CSV file and
    generate a map of choices to integers that are even powers of 2, for
    encoding the selection set efficiently as a bitmap in a single
    (potentially large) integer.
    """
    language_set = set()
    with open(csv_filename, 'rU') as f:
        csv_reader = csv.reader(f)
        headers = csv_reader.next()
        lang_index = headers.index('WantWorkLanguage')
        for row in csv_reader:
            value = row[lang_index]
            languages = [lang.strip() for lang in value.split(';')]
            language_set.update(languages)
    result = {}
    for i, language in enumerate(sorted(language_set)):
        result[language] = 2**i
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
