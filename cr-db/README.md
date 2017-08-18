Welcome to the Crunch.io db fitness test.

Here you will find a python package to help us evaluate your skills with:

1. Problem Solving
2. Scalability
3. Analytics
4. Testing

Instructions

1. Fork the repo into a private repo.
2. Create a virtualenv for this project and install the cr-api and cr-db packages into your environment.
3. Modify the cr-db package to complete the task, the code is commented with task items.
4. Let us know when you have finished.

Deliverable

Publish your work in a GitHub repository.  Please use Python 2.x for your coding.  Feel free to modify this 
readme to give any additional information a reviewer might need.

Implementation Details and Assumptions

## ``test_loader``

 1. Is this the most efficient way that we could load users? What if the
    file had 1m users?

    No. The ``load_data()`` function has a couple of inefficiencies:

    -   In doing a ``json.load()`` on the entire file, it sucks all of the
        data into memory before doing anything with it. This doesn't scale
        for really large data files or boxes with limited RAM.

        One way to deal with this is to use something like
        [ijson](https://pypi.python.org/pypi/ijson) and parse the file
        incrementally. It helps that the data file is an array of objects;
        ``ijson`` basically would let you iterate over those objects.

    -   The data is is persisted with a ``collection.insert(obj)`` call per
        item in the array. With 1m+ objects that is 1m+ separate inserts.

        The way to deal with that is to batch objects as they are
        incrementally parsed from the input file, then do
        ``collection.insert_many(batch)``.

        I would do benchmarking and experimentation to decide on a good
        batch size. I would probably start with 1000 for my first
        experiment.

 2. How would/could you benchmark this?

    For things that run in microseconds it is best to use something like
    [timeit](https://docs.python.org/2.7/library/timeit.html#module-timeit)
    or [perf](https://perf.readthedocs.io/en/latest/api.html) to measure how
    long it takes a function to run.  But for something that takes much
    longer to run, like loading 1m+ records, using ``time.time()`` as a
    stopwatch / timer works just fine for me.

    The steps I would take in this case are:

    -   Augment the test data with additional files containing 1000, 100000,
        and 1000000 (1M) records, respectively.

    -   Write a test that runs the original ``load_data()`` function against
        each of the data sets and reports the run time. If it takes the
        original function too agonizingly long or runs out of memory, the
        bigger files may have to be omitted. *This becomes the baseline for
        comparison.* Make sure the benchmark script or function can replace
        the original function being tested with another one.

    -   Come up with a hypothesis about what is taking so long. In this case
        I think the scalability limitations are obvious, but in case it is
        not, maybe run the benchmark test with profiling enabled.

    -   Create one or more alternative versions that we hope are
        improvements. _Note_: it's possible to accidentally slow things down
        when attempting to optimize!

    -   Run the benchmark test against the alternatives and compare the
        results with the baseline. Keep changes if they turn out to be
        improvements.

    -   Repeat until desired performance is achieved and/or knowledge is
        gained.

## ``test_load_dataset``

**My Assumptions**:

Looking at ``load_dataset()``, the definition of a dataset appears to be:

- List of headers
- Each header is a string or ``None``
- List of columns, one per header
- Each column is an array of values
- Each column in the dataset has the same number of values
- Each item in a column is of the same same type (after normalization)
  or ``None`` for missing value
- A dataset has a unique ID
- The entire dataset can be retrieved with that one ID

I'm assuming further that the data is immutable. The dataset is retrieved as
a unit and the columns are iterated over, but the values in them are not
changed, nor deleted nor appended to. The entire dataset can be deleted but
not individual values nor columns.

**Questions**: How would you manage a much larger dataset?  Does it make
sense to store the raw data in mongo?  What other strategies would you
employ if you had 1000s of datasets with 1 million rows per dataset?

I would not store the raw data in mongo. My understanding is that mongo has
a 16MB document size limit, and I'm assuming that 1 million rows in a
dataset would easily exceed that limit.

Given my assumptions above, I would not store the data in a relational
database. If there were complex interlinkages between data in the columns or
between the datasets, and if you had to be able to change any item in an row
at any time while still returning consistent query results to multiple
readers, then that's when I'd go with a relational database.

Instead, I would consider using mongo as a data catalog, and maybe as a
locking system to handle multiple clients uploading and deleting datasets at
the same time. The mongo database would contain pointers to the real dataset
locations.

Given my assumptions above (datasets are immutable, all data values in a
column are the same type or ``None``), this is the kind of thing that SAS
datasets were designed for. Not that I would wish SAS *software* on anyone,
but something conceptually similar for the dataset file format. If you can
assume that all data items in a given column are of the same type, you can
have fixed-width column representation, and that in turn can make it very
efficient to index column values, chunk values for streaming, marshall to
Python data types in bulk, etc.

## ``test_select_with_filter``

Answer but don't code: what would a generic solution look like to compare
any columns containing categories?

