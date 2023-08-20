const { DATABASE_SCHEMA, DATABASE_URL, SHOW_PG_MONITOR } = require('./config');
const massive = require('massive');
const monitor = require('pg-monitor');
const axios = require('axios');

// Call start
(async () => {
    console.log('main.js: before start');

    const db = await massive({
        connectionString: DATABASE_URL,
        ssl: { rejectUnauthorized: false },
    }, {
        // Massive Configuration
        scripts: process.cwd() + '/migration',
        allowedSchemas: [DATABASE_SCHEMA],
        whitelist: [`${DATABASE_SCHEMA}.%`],
        excludeFunctions: true,
    }, {
        // Driver Configuration
        noWarnings: true,
        error: function (err, client) {
            console.log(err);
            //process.emit('uncaughtException', err);
            //throw err;
        }
    });

    if (!monitor.isAttached() && SHOW_PG_MONITOR === 'true') {
        monitor.attach(db.driverConfig);
    }

    const execFileSql = async (schema, type) => {
        return new Promise(async resolve => {
            const objects = db['user'][type];

            if (objects) {
                for (const [key, func] of Object.entries(objects)) {
                    console.log(`executing ${schema} ${type} ${key}...`);
                    await func({
                        schema: DATABASE_SCHEMA,
                    });
                }
            }

            resolve();
        });
    };

    //public
    const migrationUp = async () => {
        return new Promise(async resolve => {
            await execFileSql(DATABASE_SCHEMA, 'schema');

            //cria as estruturas necessarias no db (schema)
            await execFileSql(DATABASE_SCHEMA, 'table');
            await execFileSql(DATABASE_SCHEMA, 'view');

            console.log(`reload schemas ...`)
            await db.reload();

            resolve();
        });
    };

    /* Returns filtered object list from a flat JSON object where key matches a certain value. */
    const filterFlat = (json, key, values) => {
        return json.data.filter(item => values.includes(item[key]));
    };
    
    /* Returns the sum of values that matches a property*/
    const calculateObjectPropertySum = (objects, property) => {
        return objects.map(item => item[property]).reduce((acc, value) => acc + value, 0);
    };

    /* Returns the fetched data from source */
    const fetchData = async (src) => {
        try {
            response = await axios.get(src);
            return response.data;
        }
        catch (e) {
            console.log(e.message);
            return null;
        }
    };

    const calculatePopulationByYear = (populationJson, years) => {
        const relevantData = filterFlat(populationJson, 'ID Year', years);
        return calculateObjectPropertySum(relevantData, 'Population');
    };

    const calculateTotalPopulationFromDb = async () => {
        try {
            const result = await db.query(`
                SELECT SUM((item->>'Population')::INT) AS total_population 
                FROM (
                    SELECT jsonb_array_elements(doc_record->'data')::jsonb AS item 
                    FROM ${DATABASE_SCHEMA}.api_data
                ) AS subquery 
                WHERE (item->>'ID Year')::INT IN (2020, 2019, 2018);`,
                { build: true },
                { single: true }
            );
            return parseInt(result.total_population);
        } catch (error) {
            throw error;
        }
    }

    try {
        await migrationUp();

        const populationJson = await fetchData('https://datausa.io/api/data?drilldowns=Nation&measures=Population');
        const populationFromFetch = calculatePopulationByYear(populationJson, [2020, 2019, 2018]);

        await db[DATABASE_SCHEMA].api_data.insert({
            doc_record: populationJson
        });

        const totalPopulation = await calculateTotalPopulationFromDb();
        
        //Logs results
        console.log(`Total population for the years 2020, 2019 and 2018.`)
        console.log('NodeJS:',  populationFromFetch);
        console.log('PostgreSQL:', totalPopulation);

    } catch (e) {
        console.log(e.message)
    } finally {
        console.log('finally');
    }
    console.log('main.js: after start');
})();