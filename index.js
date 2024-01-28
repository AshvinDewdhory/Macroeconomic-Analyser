const fs = require('fs');
const csv = require('csv-parser');
const filePath = 'CPI Timeseries.csv';
const axios = require('axios');
const mysql = require('mysql2/promise');
const { totalmem } = require('os');

const worldBankApiUrl = 'http://api.worldbank.org/V2';
const worldBankIndicators = [ 'ER.FSH.PROD.MT',   // Total fisheries production (metric tons)
'AG.SRF.TOTL.K2',   // Agricultural land (sq. km)
'IC.REG.DURS',      // Time required to start a business (days)
'IC.BUS.NREG',      // New businesses registered (number)
'SL.AGR.EMPL.ZS',   // Employment in agriculture (% of total employment) (modelled ILO estimate)
'SL.EMP.SELF.ZS'    // Self-employed, total (% of total employment) (modelled ILO estimate)
]; // Add more indicators as needed

const worldBankCountriesCodes = [ 'SYC',   // Seychelles
'MDV',   // Maldives
'CIV',   // Côte d'ivoire
'KEN',   // Kenya
'LKA',   // Sri Lanka
'IND',   // India
'SGP',   // Singapore
'AUS',   // Australia
'DNK',   // Denmark
'FIN'    // Finland
];

// database connection

const dbConfig = {
    host: 'localhost',
    user: 'root',
    password: '',
    database: 'macroeconomic_analyser',
};

function extractData(filePath) {
    return new Promise((resolve, reject) => {
        const data = [];
        fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', (row) => {
                data.push(row);
            })
            .on('end', () => {
                resolve(data);
            })
            .on('error', (error) => {
                reject(error);
            });
    });
}

// Step 2: Transform Data
function transformData(data) {
    // For simplicity, transform data by selecting a subset of columns and filtering by year (2020 to 2012)
    const transformedData = [];

    data.map((row) => {

        // Loop through the years in the row
        for (let year = 2020; year >= 2012; year--) {

            const obj = {
                IndicatorName : 'Corruption Perception Index',
                IndicatorCode : 'CPI',
                CountryCode: row.ISO3,
                Year: year,
                Value: row[`CPI Score ${year}`]||row[`CPI score ${year}`],
                Rank: row[`Rank ${year}`],
            }

            transformedData.push(obj);
        }

        
    });
    //console.log(transformedData)
    return transformedData;
}

// Step 3: Extract Data from World Bank API
async function extractWorldBankData() {
    try {
        let page = 1;
        let totalPages = 0;
        let results = [];

        do {
            const responses = await getWorldBankData(page);
            //console.log(responses);
            totalPages = responses.data[0].pages;

            results.push(...responses.data[1].map((response) => {
                return {
                    IndicatorName : response.indicator.value,
                    IndicatorCode : response.indicator.id,
                    CountryCode: response.countryiso3code,
                    Year: response.date,
                    Value: response.value || null,
                    Rank : null
                };
            }));

            page = page + 1;
        }
        while (page <= totalPages);
        
        //console.log(data);
        return results;
    } catch (error) {
        throw error;
    }
}

async function getWorldBankData(page) {
    const responses = (await axios.get(`${worldBankApiUrl}/country/${worldBankCountriesCodes.join(';')}/indicator/${worldBankIndicators.join(';')}?format=json&date=2019:2022&source=2&per_page=100&page=${page}`));
    //console.log(responses.data)
    return responses;
}

// Step 3: Load Data
async function loadData(data) {
    const connection = await mysql.createConnection(dbConfig);
    
    try {

        // Insert data into the table
        for (const row of data) {
            await connection.query(`
                INSERT INTO indicator_info (countryCode, year, value, Rank, name, indicatorCode)
                VALUES (?, ?, ?, ?, ?, ?)
            `, [row.CountryCode, row.Year, row.Value, row.Rank, row.IndicatorName, row.IndicatorCode]);
        }

        console.log('CPI data loaded into the MySQL database successfully.');
    } catch (error) {
        console.error('Error loading CPI data into the database:', error);
    } finally {
        // Close the database connection
        await connection.end();
    }
}

// Step 4: Update Rank Column
async function updateRank() {
    const connection = await mysql.createConnection(dbConfig);
    
    try {

        // Update Rank field in indicator_info table
        await connection.query(`
            UPDATE INDICATOR_INFO i1
               SET i1.RANK = (SELECT COUNT(*) + 1
                                FROM INDICATOR_INFO i2
                               WHERE i2.NAME = i1.NAME
                                 AND i2.YEAR = i1.YEAR
                                 AND i2.VALUE IS NOT NULL
                                 AND i2.VALUE < i1.VALUE)
        `);

        console.log('Rank field in indicator_info table updated successfully.');
    } catch (error) {
        console.error('Error loading CPI data into the database:', error);
    } finally {
        // Close the database connection
        await connection.end();
    }
}

async function runDataPipeline(filePath) {
    try {
        // Step 1: Extract Data
        const rawData = await extractData(filePath);

        // Step 2: Transform Data
        const transformedData = transformData(rawData);
        const worldBankData = await extractWorldBankData();

        // Step 3: Load Data
        loadData([...worldBankData, ...transformedData]);

    } catch (error) {
        console.error('Error in data pipeline:', error);
    }
}

//runDataPipeline(filePath);
updateRank();