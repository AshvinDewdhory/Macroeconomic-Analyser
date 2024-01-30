const fs = require('fs');
const csv = require('csv-parser');
const axios = require('axios');
const mysql = require('mysql2/promise');
const { totalmem } = require('os');

const filePath = 'CPI Timeseries.csv';
const worldBankApiUrl = 'http://api.worldbank.org/V2';
const worldBankIndicators = [
    'ER.FSH.PROD.MT',   // Total fisheries production (metric tons)
    'AG.SRF.TOTL.K2',   // Agricultural land (sq. km)
    'IC.REG.DURS',      // Time required to start a business (days)
    'IC.BUS.NREG',      // New businesses registered (number)
    'SL.AGR.EMPL.ZS',   // Employment in agriculture (% of total employment) (modelled ILO estimate)
    'SL.EMP.SELF.ZS'    // Self-employed, total (% of total employment) (modelled ILO estimate)
];

// database connection
const dbConfig = {
    host: 'localhost',
    user: 'root',
    password: '',
    database: 'macroeconomic_analyser',
};

// Extract CPI Timeseries.csv Data
function extractCsvData(filePath) {
    console.log('Starting csv extraction');
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

function transformCsvData(data) {
    console.log('Transforming csv data');
    const transformedData = [];

    data.map((row) => {
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
    return transformedData;
}

async function getWorldBankData(page) {
    const responses = (await axios.get(`${worldBankApiUrl}/country/all/indicator/${worldBankIndicators.join(';')}?format=json&date=2019:2022&source=2&per_page=100&page=${page}`));
    return responses;
}

async function extractWorldBankData() {
    console.log('Extracting world bank data');
    try {
        let page = 1;
        let totalPages = 0;
        let results = [];

        do {
            const responses = await getWorldBankData(page);
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

async function loadData(data) {
    console.log('Inserting into databse');
    const connection = await mysql.createConnection(dbConfig);
    
    try {
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
        await connection.end();
    }
}

async function updateRank() {
    console.log('Updating rank field');
    const connection = await mysql.createConnection(dbConfig);
    
    try {
        // Update Rank field in indicator_info table
        await connection.query(`
            UPDATE INDICATOR_INFO i1
               SET i1.RANK = (SELECT COUNT(*) + 1
                                FROM INDICATOR_INFO i2
                               WHERE i2.NAME = i1.NAME
                                 AND i2.YEAR = i1.YEAR
                                 AND i2.VALUE > i1.VALUE)
              WHERE i1.INDICATORCODE != 'CPI'
                AND i1.VALUE IS NOT NULL
        `);

        console.log('Rank field in indicator_info table updated successfully.');
    } catch (error) {
        console.error('Error updating Rank column in indicator_info table.', error);
    } finally {
        await connection.end();
    }
}

async function runDataPipeline(filePath) {
    try {
        console.log('Starting data pipeline');
        const csvData = await extractCsvData(filePath);
        const transformedCsvData = transformCsvData(csvData);
        const worldBankData = await extractWorldBankData();

        // Insert data in indicator_info table
        await loadData([...worldBankData, ...transformedCsvData]);

        // Update Rank column in indicator_info table
        await updateRank(); 
        console.log('Ending data pipeline');
    } catch (error) {
        console.error('Error in data pipeline:', error);
    }
}

runDataPipeline(filePath);