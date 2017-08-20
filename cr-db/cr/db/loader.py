import csv
import os
import json
import sys

from cr.db.rules import get_converter_funcs
from cr.db.store import global_settings, connect

def load_data(filename, settings=None, clear=None):
    if settings is None:
        settings = global_settings
        global_settings.update(json.load(file(sys.argv[1])))

    db = connect(settings)

    obj_name = os.path.basename(filename).split('.')[0]

    collection = getattr(db, obj_name)

    if clear:
        collection.remove()

    with file(filename) as the_file:
        objs = json.load(the_file)
        for obj in objs:
            collection.insert(obj)


def load_dataset_to_dict(csv_filename):
    with file(csv_filename, 'rU') as csv_file:
        csv_data = csv.reader(csv_file)
        headers = csv_data.next()

        last_header = None
        for i, header in enumerate(headers):
            if header:
                last_header = header
            else:
                # multiple response have no header
                headers[i] = last_header

        columns = [[] for _ in headers]
        converter_funcs = get_converter_funcs(headers)
        for r, row in enumerate(csv_data):
            for i, data in enumerate(row):
                columns[i].append(converter_funcs[i](data))

        data = {'headers': headers,
                'columns': columns,
                }
        return data


def load_dataset(csv_filename, db):
    data = load_dataset_to_dict(csv_filename)
    return db.datasets.insert(data)
