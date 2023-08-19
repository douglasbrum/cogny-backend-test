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

    const fetchData = async () => {
        try {
            response = await axios.get('https://datausa.io/api/data?drilldowns=Nation&measures=Population');
            return response;
        }
        catch (e) {
            console.log(e.message);
            return null;
        }
    };

    const calculateTotalPopulationFromNode = async (jsonData) => {
        const relevantYears = [2020, 2019, 2018];
        const relevantData = jsonData.data.filter(item => relevantYears.includes(item['ID Year']));
        const totalPopulation = relevantData.map(item => item.Population).reduce((acc, population) => acc + population, 0);
        return totalPopulation;
    }

    try {
        await migrationUp();

        const record_value = await fetchData();
        const totalPopulation = await calculateTotalPopulationFromNode(record_value.data);
        console.log(`The total population for the years 2020, 2019, and 2018 calculated from NodeJS is ${totalPopulation}`);

        //exemplo de insert
        const result1 = await db[DATABASE_SCHEMA].api_data.insert({
            doc_record: { 'a': 'b' },
        })
        console.log('result1 >>>', result1);

        //exemplo select
        const result2 = await db[DATABASE_SCHEMA].api_data.find({
            is_active: true
        });
        console.log('result2 >>>', result2);

    } catch (e) {
        console.log(e.message)
    } finally {
        console.log('finally');
    }
    console.log('main.js: after start');
})();