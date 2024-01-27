const fs = require('fs');
const csv = require('csv-parser');
const filePath = 'CPI Timeseries.csv';
const axios = require('axios');
const mysql = require('mysql2/promise');

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
        const responses = (await axios.get(`${worldBankApiUrl}/country/${worldBankCountriesCodes.join(';')}/indicator/${worldBankIndicators.join(';')}?format=json&date=2019:2022&source=2`)).data[1];
        
        const data = responses.reduce((result, response, index) => {
            //console.log(result, response, index);

            responses.forEach(entry => {
                result.push({
                    IndicatorName : response.indicator.value,
                    IndicatorCode : response.indicator.id,
                    CountryCode: entry.countryiso3code,
                    Year: entry.date,
                    Value: entry.value || null,
                    Rank : null
                });
            });
            //console.log(result);
            return result;
        }, []);
        //console.log(data);
        return data;
    } catch (error) {
        throw error;
    }
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

runDataPipeline(filePath);