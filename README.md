# rutio-tsdb
Time Series Database on top of sqlapi2.

>
> A simple object time series database which stores typed values and objects in a table per type (except objects which are distributed).
>
> Timestamps are 64 bit with ms resolution.
>
> Query method exist for requesting an object state at a particular time, or for retreiving a particular series from an object.
>
> Objects in the database are represented with a 32bit key.
>
> Database tables are optionally locked by setting the environment variable TSDB_LOCK_TABLES to non-zero value.
>
> Data does not have to be inserted "in order of occurrence". Also partial data updates are supported.
> The API includes retreiving a particular series, or objects state at a particular time.
> 

## Example use

1. Set up the DB_HOST, DB_NAME, DB_USER, DB_PASSWORD environment variables as per your mysql database.
The user must have CREATE access to the schema, since this will create the necessary 5 tables in the database when it is run, it also needs read and write access to the named database.

2. Optionally set up the TSDB_LOCK_TABLES in case you might have multiple clients running in parallell. 
In that case you also need to give the LOCK TABLES right to the user.

3. Code example

```

const tsdb = require('rutio-tsdb');

const test = () => {
    // Create the required tables. They will be prefixed by example and named example_ts_number, example_ts_string, 
    // example_ts_array, example_ts_date and example_ts_boolean, where example is from the script below. This prefixing
    // enables storage of different and disjoint series in the same database.
    await tsdb.initialize('example'); 

    // Objects are identified by a 32 bit integer unique ID (TBD: wrap this with something that maps other id formats to such integers?)

    const myId = 1;

    // Insert an object in the database
    const firstTime = new Date();
    await tsdb.insertObject(myId, {a:1, b:"Hello", c: true, d: new Date(), e:[1,2,3]} , firstTime);

    // Update one attribute of the object in the database with a new timestamp
    const secondTime = new Date(firstTime.getTime()+1000);
    await tsdb.insertObject(myId, {a:2}, secondTime);

    // Read out the object from the database
    const current = await tsdb.synthesizeObject(myId);
    console.log("At now", current);

    // Read out the object state from first time instead
    const first = await tsdb.synthesizeObjectAt(myId, firstTime);
    console.log("At start time", first);

    // Read out the time series for attribute d (note the prefix .) for my object
    const series = await tsdb.getSeries(myId, '.d', secondTime, /* LIMIT*/ 3);
    console.log("All values for .d", series);

    // Note that repeated runs of this script will also print data from previous runs...
}
```

## Dependencies

This is built on top of a simple async api to mysql2, sqlapi (from same author), available at https://github.com/ruti-se/sqlapi

