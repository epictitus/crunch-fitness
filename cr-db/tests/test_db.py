"""
Test Module for the crunch persistence.

hint: we use py.test.
"""
from __future__ import print_function

import itertools
import operator
import os
import textwrap

import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt
import numpy as np

from cr.db.loader import load_data, load_dataset
from cr.db.rules import (
    BitmappedSetColumn,
    CATEGORY_FORMAL_EDUCATION,
    CATEGORY_GENDER,
)
from cr.db.store import global_settings as settings
from cr.db.store import connect

settings.update({"url": "mongodb://localhost:27017/test_crunch_fitness"})
db = connect(settings)

_here = os.path.dirname(__file__)

def test_loader():
    """
    Is this the most efficient way that we could load users?  What if the file had 1m users?
    How would/could you benchmark this?
    -> See my answers in README.md
    """

    load_data(_here + '/data/users.json', settings=settings, clear=True)
    assert db.users.count() == 10, db.users.count()

def test_load_dataset():

    #data_filename = _here + '/data/Stack-Overflow-Developer-Survey-2017.csv.zip'

    #if not os.path.exists(csv_filename):
    #    with zipfile.ZipFile(data_filename, 'r') as zipref:
    #        zipref.extractall(_here + '/data')

    # First clear previous test dataset(s)
    db.datasets.drop()

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
    assert all(isinstance(header, basestring) for header in headers)

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
        # Treat int and long as equivalent (not a problem on Python 3)
        if issubclass(column_type, (int, long)):
            column_type = (int, long)
        assert all(isinstance(item, column_type) or item is None for item in column)

    # the columns aren't terribly useful.  Modify load_dataset to load common responses as integers so we can
    #   do data manipulation.  For instance, you could change the gender column to male = 0 female = 1 (or something)

    # you _should_ be able to save S-O-10k if you convert booleans to boolean and use integers for categories.
    # -> See my comments in README.md about what happened when I took this
    # at face value. Spoiler alert: It couldn't be done. But I tried anyway.

    # how would you manage a much larger dataset?  Does it make sense to store the raw data
    #   in mongo?  What other strategies would you employ if you had 1000s of datasets with 1 million rows per dataset?
    # -> See my answers in README.md


def test_bitmapped_set_column():
    column = BitmappedSetColumn([
        # Order is super important!
        "Apple",        # 1
        "Banana",       # 2
        "Cucumber",     # 4
        "Pear",         # 8
    ])
    assert column('Apple') == 1
    assert column('Banana; Pear') == 10
    assert column('Apple; Cucumber; Pear') == 13
    assert column[1] == 'Apple'
    assert column[10] == 'Banana; Pear'
    assert column[13] == 'Apple; Cucumber; Pear'


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

    # Load and save S-0-5k
    csv_filename = _here + '/data/S-O-5k.csv'

    ds_id = load_dataset(csv_filename, db)

    dataset = db.datasets.find({'_id': ds_id})[0]
    headers = dataset['headers']
    columns = dataset['columns']

    gender_col = columns[headers.index('Combined Gender')]
    salary_col = columns[headers.index('SalaryAdjusted')]
    education_col = columns[headers.index('FormalEducation')]
    female_code = CATEGORY_GENDER('female')
    assert female_code is not None

    def _generate_data():
        for gender, salary, education in itertools.izip(gender_col,
                                                        salary_col,
                                                        education_col):
            if gender != female_code:
                continue
            if salary is None:
                continue
            yield education, salary

    print(gender_col.count(female_code), "female developers in the dataset.")
    data_array = np.rec.array(np.fromiter(
        _generate_data(),
        dtype=[('education', 'i4'), ('salary', 'f8')],
    ), copy=False)
    data_array.sort()
    # Sanity check
    print(len(data_array), "female developers reported salary.")
    assert len(data_array) > 0

    # Generate labels
    x_range = np.arange(data_array.education.min(), data_array.education.max() + 1)
    x_lowest = x_range[0]
    x_highest = x_range[-1]
    labels = [CATEGORY_FORMAL_EDUCATION[i]
              for i in xrange(x_lowest, x_highest + 1)]
    x_labels = ['\n'.join(textwrap.wrap(label, width=25)) for label in labels]
    x_labels.insert(0, '')  # required by matplotlib, I don't know why

    # Rebase x values on zero so bincount will work properly
    x_range -= x_lowest
    data_array.education -= x_lowest

    # Compute mean salaries
    salary_mean = []
    i = 0
    for count in np.bincount(data_array.education):
        salary_mean.append(data_array.salary[i:i+count].mean())
        i += count

    # Plot the data
    fig = plt.figure(figsize=(10.24, 7.68), dpi=100)
    nrows, ncols, axnum = 1, 1, 1
    ax = fig.add_subplot(nrows, ncols, axnum)
    ax.set_title("Female Developers Salary by Formal Education")
    ax.set_xlabel("Formal Education")
    ax.set_ylabel("Adjusted Salary")
    ax.plot(data_array.education, data_array.salary, 'ro')
    ax.plot(x_range, salary_mean)
    ax.set_xticklabels(x_labels, rotation='vertical')
    # Tweak spacing to prevent clipping of tick-labels
    fig.subplots_adjust(bottom=0.30)
    report_filename = 'female-developers-salary-by-education.png'
    print("Saving report output to", report_filename)
    fig.savefig(report_filename)
    plt.close(fig)


def _test_load_large_dataset_with_benchmark():
    """notes for later: ignore me"""

    #data_filename = _here + '/data/Stack-Overflow-Developer-Survey-2017.csv.zip'

    #if not os.path.exists(csv_filename):
    #    with zipfile.ZipFile(data_filename, 'r') as zipref:
    #        zipref.extractall(_here + '/data')

    #csv_filename = _here + '/data/S-O-1k.csv'

    #ds_id = load_dataset(csv_filename, db)

