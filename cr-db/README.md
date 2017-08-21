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

**Questions**

> How would you manage a much larger dataset?  Does it make sense to store
> the raw data in mongo?  What other strategies would you employ if you had
> 1000s of datasets with 1 million rows per dataset?

I would not store the raw data in mongo. Mongo has a 16MB document size
limit. 1 million rows in a dataset would easily exceed that limit.

Given my assumptions above, I would not store the data in a relational
database. I would only use a relation database if:

- There were complex interlinkages between data in the columns or between
  the datasets
- We have to be able to change any item in an row at any time while still
  returning consistent query results to multiple readers

I would consider using mongo as a data catalog, to store meta-data about the
datasets, and as a management system to handle multiple clients uploading
and deleting datasets at the same time. The mongo database would contain
pointers to the real dataset locations.

For storing the raw, un-cleansed datasets, I would store them just as a file
in either object storage like S3 or an NFS-like filesytem.

For cleansed datasets where column values have been normalized, it occurred
to me that this is the kind of thing that the SAS dataset file format was
designed for. (To be clear, I'm not recommending you use SAS *software*.)
The file format (at least the version I was familar with) has fixed-width
column storage. Fixed-width columns in binary format can make it very
efficient to index column values, chunk values for streaming, quickly
marshall to Python data types, etc.

When googling "Mongo BSON document too large" I ran across
[GridFS](https://docs.mongodb.com/manual/core/gridfs/):

> GridFS is a specification for storing and retrieving files that exceed the
> BSON-document size limit of 16 MB.

> Instead of storing a file in a single document, GridFS divides the file
> into parts, or chunks [1], and stores each chunk as a separate document.
> By default, GridFS uses a chunk size of 255 kB; that is, GridFS divides a
> file into chunks of 255 kB with the exception of the last chunk. The last
> chunk is only as large as necessary. Similarly, files that are no larger
> than the chunk size only have a final chunk, using only as much space as
> needed plus some additional metadata.

GridFS basically lets you use your mongo cluster as a file server. However,
I'm still not sure about putting *all* of the eggs in the mongo basket.
It could be an advantage to let applications access datasets as files or
HTTP streams without going through the mongo server nor using a mongo client
driver.

**Issues**: I perhaps mistakenly took this statement in the test comments at
face value:

    # you _should_ be able to save S-O-10k if you convert booleans to boolean and use integers for categories.

So I tried to make sure I could load 10k rows. My assumption was that this
was needed for the next step, ``test_select_with_filter``, to get enough
usable values (since there are relatively few female programmers.)

However, there were several problems with loading the S-O-10k.csv file:

-   S-O-10k.csv has no header row. I created S-O-10k-with-headers.csv from
    the first 10000 rows in Stack-Overflow-Developer-Survey-2017.csv to
    remedy that problem.

-   The math doesn't really work out. Consider that there are 415 columns
    and 10000 rows, that results in 4.15M cells. That gives us only an
    average of 4.04 bytes per cell to make the 16M limit. That's pretty
    harsh.
  
    But, I rationalized, if all the categories are converted to integers,
    and Mongo economizes on storage of null and boolean and small integers,
    I figured it could be possible. Barely. So I decided to "go for broke"
    and convert all category values to enumerated integers.

-   However, things got more difficult when I got to the
    ``WantWorkLanguage`` column.  It is a set of programming languages,
    represented in the CSV file as nasty long strings of semicolon-separated
    values, like this:

        "Assembly; C#; Elixir; Go; Haskell; JavaScript; Lua; PHP; R; Ruby"

    The value is pretty much unique for each programmer. Feeling really
    committed at this point, I handled this column as a set encoded with a
    bitmapped integer. Please see my implementation in
    ``BitmappedSetColumn`` in cr/db/rules.py. I added unit tests for this
    class to test_db.py to confirm that my implementation is lossless and
    round-trippable.

-   Even after all my hard work of converting literally *every* cell from a
    string to numeric representation, I still couldn't make it under the 16M
    limit. My final calculated dataset size (see ``calc_dataset_size()`` in
    the ``cr.db.helper`` modules) was 25581143 bytes, still 1.52 times the
    16MB limit. I hope I get points for effort here!

## ``test_select_with_filter``

**Implementation Notes**

I took the first 5000 lines from Stack-Overflow-Developer-Survey-2017.csv to
create S-O-5k.csv for my sample. There were 151 female developers in the
dataset, 52 of whom reported their salaries.

After running ``pytest``, please see the output report in
[female-developers-salary-by-education.png](female-developers-salary-by-education.png).

**Question**

> Answer but don't code: what would a generic solution look like to compare
> any columns containing categories?
