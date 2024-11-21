// Example use of the time series DB

// Normally read from environment
process.env.DB_NAME='test';
process.env.DB_USER='test';
process.env.DB_HOST='localhost';
process.env.DB_PASSWORD='test123';
process.env.TSDB_ENABLE_LOCK=0;

// Note, would be require('rutio-tsdb') when installing through yarn or npm
// const tsdb = require('rutio-tsdb');
const tsdb = require('./tsdb.js');

const test = async () => {

    try {
        // Create the required tables. They will be prefixed by example and named example_ts_number, example_ts_string, 
        // example_ts_array, example_ts_date and example_ts_boolean, where example is from the script below. This prefixing
        // enables storage of different and disjoint series in the same database.
        await tsdb.initialize('example'); 

        // Objects are identified by a 32 bit integer unique ID (TBD: wrap this with something that maps other id formats to such integers?)

        const myId = 'id-1';

        // Insert an object in the database
        const firstTime = new Date();
        await tsdb.insertObject(myId, {a:1, b:"Hello", c: true, d: firstTime, e:[1,2,3]} , firstTime);

        // Update one attribute of the object in the database with a new timestamp
        const secondTime = new Date(firstTime.getTime()+1000);
        await tsdb.insertObject(myId, {a:2}, secondTime);

        // Read out the object from the database
        const current = await tsdb.synthesizeObject(myId);
        console.log("At now", current);

        // Read out the object state from first time instead
        const first = await tsdb.synthesizeObjectAt(myId, firstTime);
        console.log("At start time", first);

        // Read out the time series for attribute a (note the prefix .) for my object
        const limit = 5;
        const series = await tsdb.getSeries(myId, '.a', secondTime, limit);
        console.log(`Last ${limit} values for .a`, series);

        // Search for all items with field .d = firstTime
        const found = await tsdb.search('.d', firstTime, secondTime, limit);
        console.log(`Found items with .d=${firstTime.toISOString()}:`, found);

        // Note that repeated runs of this script will also print data from previous runs...

    } catch (e) {
        console.log(e);
    }
}

test();
