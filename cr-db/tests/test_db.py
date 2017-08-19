"""
Test Module for the crunch persistence.

hint: we use py.test.
"""

import os
from cr.db.loader import load_data, load_dataset
from cr.db.store import global_settings as settings
from cr.db.store import connect

settings.update({"url": "mongodb://localhost:27017/test_crunch_fitness"})
db = connect(settings)

_here = os.path.dirname(__file__)

def test_loader():
    """
    Is this the most efficient way that we could load users?  What if the file had 1m users?
    How would/could you benchmark this?
    -> See my anwers in README.md
    """

    load_data(_here + '/data/users.json', settings=settings, clear=True)
    assert db.users.count() == 10, db.users.count()

def test_load_dataset():

    #data_filename = _here + '/data/Stack-Overflow-Developer-Survey-2017.csv.zip'

    #if not os.path.exists(csv_filename):
    #    with zipfile.ZipFile(data_filename, 'r') as zipref:
    #        zipref.extractall(_here + '/data')

    csv_filename = _here + '/data/S-O-1k.csv'

    ds_id = load_dataset(csv_filename, db)

    dataset = db.datasets.find({'_id': ds_id})[0]
    headers = dataset['headers']
    columns = dataset['columns']

    # These assertions verify many of my assumptions about the nature of a
    # non-empty dataset.

    # There is at least one header
    assert len(headers) > 0

    # Each header is a string or None
    assert all(isinstance(header, basestring) or header is None
               for header in headers)

    # There is exactly one column per header
    assert len(headers) == len(columns)

    # There is at least one row
    assert len(columns[0]) > 0

    # Each column has the same number of rows
    assert all(len(column) == len(columns[0]) for column in columns)

    # Each item in a given column is of the same type, or None
    for column in columns:
        column_type = None
        for item in column:
            if item is not None:
                column_type = type(item)
                break
        else:
            # All None values - I guess that is Ok
            # Go check the next column
            continue
        assert all(type(item) == column_type or item is None for item in column)

    # the columns aren't terribly useful.  Modify load_dataset to load common responses as integers so we can
    #   do data manipulation.  For instance, you could change the gender column to male = 0 female = 1 (or something)

    # you _should_ be able to save S-O-10k if you convert booleans to boolean and use integers for categories.

    # how would you manage a much larger dataset?  Does it make sense to store the raw data
    #   in mongo?  What other strategies would you employ if you had 1000s of datasets with 1 million rows per dataset?
    # -> See my answers in README.md


def test_select_with_filter():
    """Provide a test to answer this question:
       "For women, how does formal education affect salary (adjusted)?"

       Hint: use Combined Gender to filter for women.

       The task is to load the appropriate columns in to numpy and provide a table of results,
       or better, plot with matplotlib appropriately.  Be careful about the "missing" (None) data.

       Answer but don't code: what would a generic solution look like to compare any columns containing categories?

    """
    # First clear previous test dataset(s)
    db.datasets.drop()

    # Load and save S-0-10k
    csv_filename = _here + '/data/S-O-10k.csv'

    ds_id = load_dataset(csv_filename, db)

    dataset = db.datasets.find({'_id': ds_id})[0]
    headers = dataset['headers']
    columns = dataset['columns']


def _test_load_large_dataset_with_benchmark():
    """notes for later: ignore me"""

    #data_filename = _here + '/data/Stack-Overflow-Developer-Survey-2017.csv.zip'

    #if not os.path.exists(csv_filename):
    #    with zipfile.ZipFile(data_filename, 'r') as zipref:
    #        zipref.extractall(_here + '/data')

    #csv_filename = _here + '/data/S-O-1k.csv'

    #ds_id = load_dataset(csv_filename, db)

