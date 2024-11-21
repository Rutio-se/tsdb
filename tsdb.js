// Copyright (C) 2024, Rutio AB, All rights reserved
// Author: Lars Mats

// Time series database implementation on top of mysql.

const sqlapi = require('./sqlapi');
const {isDate} = require('util/types');

const MAX_FIELD_SIZE = 512;
const MAX_STRING_SIZE = 4096;
const MAX_ARRAY_SIZE = 16384;

const TSDB_ENABLE_LOCK = process.env.TSDB_ENABLE_LOCK ? true : false;

const TS_TABLES = {
    string : 'ts_string', 
    number : 'ts_number',
    array : 'ts_array',
    date : 'ts_date',
    boolean : 'ts_boolean',
};

let tsdbTablePrefix = undefined;

const getTableForType = (type) => {
    if (!tsdbTablePrefix)
        throw (`tsdb: Call initialize before accessing tables`);
    if (!TS_TABLES.hasOwnProperty(type))
        throw (`tsdb: No table known for type ${type}`);
    return tsdbTablePrefix + "_" + TS_TABLES[type]
}

// Data representation for the various items
const TS_TABLE_VALUE_TYPES = {
    string : 'TEXT',
    number : 'DOUBLE',
    array  : 'TEXT',
    date   : 'BIGINT',
    boolean : 'TINYINT',
}


const lock = async (type) => {
    const allTypes = Object.keys(TS_TABLES);
    let tables = [];
    allTypes.map(t=>tables.push(getTableForType(t)));
    if (TSDB_ENABLE_LOCK) {
        const q = `LOCK TABLES ${tables.join(" " + type + ", ") + " " + type + ";"} `;
        await sqlapi.query(q);
    }
}

const unlock = async () => {
    if (TSDB_ENABLE_LOCK) {
        const q = `UNLOCK TABLES;`
        await sqlapi.query(q);
    }
}

const createTable = async (name, type) => {
    const valuetype = TS_TABLE_VALUE_TYPES[type];
    if (!valuetype)
        throw {message:`tsdb: No storage type defined for type ` + type};
    const q = `CREATE TABLE ${name} (
        \`id\` int unsigned NOT NULL AUTO_INCREMENT,
        \`node\` int,
        \`value\` ${valuetype},
        \`field\` varchar(${MAX_FIELD_SIZE}),
        \`timestamp\` bigint,
        \`latest\` tinyint,
        PRIMARY KEY (\`id\`),
        UNIQUE KEY \`id_UNIQUE\` (\`id\`),
        KEY \`id_node\` (\`node\`),
        KEY \`id_field\` (\`field\`),
        KEY \`id_latest\` (\`latest\`)
      ) ENGINE=InnoDB AUTO_INCREMENT=26264 DEFAULT CHARSET=utf8mb4;`;
    console.log(`tsdb: Creating table ${name} for type ${type}`, q);
    await sqlapi.query(q);
}

const createTables = async () => {
    const types = Object.keys(TS_TABLES);
    for (let i = 0; i < types.length; ++i) {
        const type = types[i];
        const table = getTableForType(type);
        const q = `SHOW TABLES LIKE ${sqlapi.escape(table)};`;
        let result = await sqlapi.query(q);
        if (result.length != 0) {
            console.log(`tsdb: Table ${table} for type ${type} exists`);
            continue;
        }
        console.log(`tsdb: Table ${table} for type ${type} does not exist, creating...`);
        await createTable(table, type);
    }
}

const morerecents = async (table, node, field, timestamp_ms) => {
    const q = `SELECT id FROM ${table} WHERE node=${node} AND timestamp > ${sqlapi.escape(timestamp_ms)} AND FIELD=${sqlapi.escape(field)} AND latest=1`;
    return await sqlapi.query(q);
}

const checkIsLatest = async (table, node, field, timestamp_ms) => {
    const morerecent = await morerecents(table, node, field, timestamp_ms);
    if (morerecent.length > 0)
        return 0; // This was not the latest value
    else {
        return 1; // This was the latest value
    }
}

const setIsLatest = async (table, node, field, id, latest) => {
    if (!latest)
        return;
    const query = `UPDATE ${table} SET latest=0 WHERE node=${sqlapi.escape(node)} AND field=${sqlapi.escape(field)} AND latest=1 AND id<>${id};`;
    await sqlapi.query(query);
}

const insertCheckedNumber = async (node, field, value, timestamp) => {
    const timestamp_ms = timestamp.getTime();
    const table = getTableForType("number");
    const latest = await checkIsLatest(table, node, field, timestamp_ms);
    const result = await sqlapi.insert(table, {node, field, value, timestamp:timestamp_ms, latest});
    setIsLatest(table, node, field, result, latest);
    return result;
}

const insertCheckedBoolean = async (node, field, value, timestamp) => {
    const timestamp_ms = timestamp.getTime();
    const table = getTableForType("boolean");
    const latest = await checkIsLatest(table, node, field, timestamp_ms);
    const result = await sqlapi.insert(table, {node, field, value : value ? 1 : 0, timestamp:timestamp_ms, latest});
    setIsLatest(table, node, field, result, latest);
    return result;
}

const insertCheckedString = async (node, field, value, timestamp) => {
    const timestamp_ms = timestamp.getTime();
    if (value.length > MAX_STRING_SIZE)
        throw {message:`tsdb: String value for field ${field} of length ${value.length} exceeds max length ${MAX_STRING_SIZE}`};
    const table = getTableForType("string");
    const latest = await checkIsLatest(table, node, field, timestamp_ms);
    const result = await sqlapi.insert(table, {node, field, value, timestamp:timestamp_ms, latest});
    await setIsLatest(table, node, field, result, latest);
    return result;
}

const insertCheckedArray = async (node, field, value, timestamp) => {
    const timestamp_ms = timestamp.getTime();
    const representation = JSON.stringify(value);
    if (representation.length > MAX_ARRAY_SIZE)
        throw {message:`tsdb: String value for field ${field} of representation length ${representation.length} exceeds max length ${MAX_ARRAY_SIZE}`};
    const table = getTableForType("array");
    const latest = await checkIsLatest(table, node, field, timestamp_ms);
    const result = await sqlapi.insert(table, {node, field, value:representation, timestamp:timestamp_ms, latest});
    await setIsLatest(table, node, field, result, latest);
    return result;
}

const insertCheckedDate = async (node, field, value, timestamp) => {
    const timestamp_ms = timestamp.getTime();
    const value_ms = value.getTime();
    const table = getTableForType("date");
    const latest = await checkIsLatest(table, node, field, timestamp_ms);
    const result = await sqlapi.insert(table, {node, field, value:value_ms, timestamp:timestamp_ms, latest});
    await setIsLatest(table, node, field, result);
    return result;
}

// Iterate over all members
const insertCheckedObject = async (node, field, value, timestamp) => {
    let fields = Object.keys(value);
    let promises = [];
    for (let i = 0; i < fields.length; ++i) {
        let subfield = field + "." + fields[i];
        promises.push(insert(node, subfield, value[fields[i]], timestamp));
    }
    await Promise.all(promises);
}

const insert = async (node, field, value, timestamp) => {
    if (!isDate(timestamp))
        throw {message: 'tsdb: timestamp of type ' + typeof(timestamp) + " is not a date"};
    if (!Number.isInteger(node))
        throw {message: "tsdb: node of type " + typeof(node) + " is not an integer number"};

    if (typeof(field) !== 'string' || field.length == 0 || field.length > MAX_FIELD_SIZE)
        throw {message: "tsdb: field of type " + typeof(field) + " is not a string or has bad length: " + field};

    if (typeof(value) == 'number' && isFinite(value)) {
        // Inserting a finite number in database
        await (insertCheckedNumber(node, field, value, timestamp));
    } else if (typeof(value) == 'string') {
        // Inserting a string in database
        await (insertCheckedString(node, field, value, timestamp));
    } else if (typeof(value) == 'boolean') {
        // Inserting a string in database
        await (insertCheckedBoolean(node, field, value, timestamp));
    } else if (isDate(value)) {
        // Inserting a date in database
        // Inserting a string in database
        await (insertCheckedDate(node, field, value, timestamp));
    } else if (Array.isArray(value)) {
        // Inserting an array in database
        await (insertCheckedArray(node, field, value, timestamp));
    } else if (typeof(value == 'object')) {
        insertCheckedObject(node, field, value, timestamp);
    } else {
        throw {message: 'tdsb: no support for value of type ' + typeof(value)}
    }
}

const convertToObject = (mapObject) => {
    let keys = Object.keys(mapObject).sort();
    let result = {};
    for (let i = 0; i < keys.length; ++i) {
        let key = keys[i];
        const path = key.split('.');
        let o = result;

        // Create object substructure if required
        for (let p = 1; p < path.length-1; ++p) {
            const field = path[p];
            if (o.hasOwnProperty(field)) {
                if (typeof(o[field]) !== 'object') {
                    // Strange case, there is both object and none-object at this location
                    throw {message:"tsdb: the object has value and object at same location: " + key + " o[p]:" + o[p]};
                }
                o = o[field];
            }
            else {
                const tmp = {}
                o[path[p]] = tmp;
                o = tmp;
            } 
        }

        const type = mapObject[key].type;
        const timestamp = new Date(mapObject[key].timestamp);
        if (type === 'number' || type === 'string')
            o[path[path.length-1]] = {value:mapObject[key].value, timestamp};
        else if (type === 'boolean')
            o[path[path.length-1]] = {value:mapObject[key].value ? true : false, timestamp};
        else if (type === 'date')
            o[path[path.length-1]] = {value: new Date(mapObject[key].value), timestamp};
        else if (type === 'array')
            o[path[path.length-1]] = {value:JSON.parse(mapObject[key].value), timestamp};
        else 
            throw {message: "tsdb: Cannot convert " + type + " to object"};
    }
    return result;
}


const synthesizeObjectParameterized = async (node, condition) => {
    if (!Number.isInteger(node))
        throw {message:"tsdb: synthesizeObject: node parameter must be an integer."};
    
    const mapLatest = {};
    const types = Object.keys(TS_TABLES);
    const promises = [];
    for (let i = 0; i < types.length; ++i) {
        const type = types[i];
        const table = getTableForType(type);
        const asyncpart = async () => {
            const rows = await sqlapi.query(`SELECT field, value, timestamp FROM ${table} WHERE node=${sqlapi.escape(node)} AND ${condition} ORDER BY timestamp ASC`);
            for (let r = 0; r < rows.length; ++r) {
                const row = rows[r];
                mapLatest[row.field] = {value: row.value, type, timestamp: row.timestamp};
            }
        }
        promises.push(asyncpart());
    }

    await Promise.all(promises);
    const synthesized = convertToObject(mapLatest);
    return synthesized;
}

exports.synthesizeObjectAt = async (node, date) => {
    try {
        await lock("READ");
        const date_ms = date.getTime();
        return synthesizeObjectParameterized(node, "timestamp<="+date_ms);
    } finally {
        await unlock();
    }
}

exports.synthesizeObject = async (node) => {
    try {
        await lock("READ");
        return synthesizeObjectParameterized(node, "latest<>0");
    } finally {
        await unlock();
    }
}

exports.initialize = async (prefix) => {
    if (typeof(prefix) !== 'string' || prefix.length == 0)
        throw {message: "tsdb: no table prefix"}
    tsdbTablePrefix = prefix;
    await createTables();
}

exports.insertObject = async (node, value, timestamp) => {
    if (typeof(value) !== 'object')
        throw {message: 'tsdb: inserted object is not an object'};
    try {
        await lock("WRITE");
        return await insertCheckedObject(node, '', value, timestamp);
    } catch (e) {
        throw e;
    } finally {
        await unlock();
    }
}

exports.getSeries = async (node, field, when, limit) => {
    if (!Number.isInteger(node))
        throw {message:'tsdb: node must be integer'};
    if (!Number.isInteger(limit) || limit < 1 || limit > 10001)
        throw {message:'tsdb: limit must be a natural number, less than 10001'};
    if (typeof(field) !== 'string')
        throw {message: 'tsdb: field must be string'};
    if (!isDate(when))
        throw {message: 'tsdb: when must be a date'};
    let when_ms = when.getTime();

    try {
        lock("READ");
        // Return results from the first DB table which gives values for the selected field    
        const types = Object.keys(TS_TABLES);
        for (let i = 0; i < types.length; ++i) {
            const type = types[i];
            const table = getTableForType(type);
            const query = `SELECT value, timestamp from ${table} WHERE node=${sqlapi.escape(node)} AND field=${sqlapi.escape(field)} AND timestamp <= ${when_ms} ORDER BY timestamp DESC LIMIT ${limit};`;
            const rows = await sqlapi.query(query);
            if (rows.length > 0) {
                for (let r = 0; r < rows.length; ++r) {
                    rows[r].timestamp = new Date(rows[r].timestamp);
                    if (type === 'array')
                        rows[r].value = JSON.parse(rows[r].value);
                    if (type === 'date')
                        rows[r].value = new Date(rows[r].value);
                    if (type === 'boolean')
                        rows[r].value = rows[r].value ? true : false;
                }
                return rows;
            }
        }
        return [];
    } finally {
        unlock();
    }
}
