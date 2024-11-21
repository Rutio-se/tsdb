# tsdb
Time Series Database on top of sqlapi2

> A simple time series database which stores typed values and objects in a table per type (except objects which are distributed).
> Timestamps are 64 bit with ms resolution.
> Query method exist for requesting an object state at a particular time, or for retreiving a particular series from an object.
> Objects in the database are represented with a 32bit key.
> Database tables are optionally locked by setting the environment variable TSDB_LOCK_TABLES to non-zero value.

# Dependencies

This is built on top of a simple async api to mysql2, sqlapi (from same author)
