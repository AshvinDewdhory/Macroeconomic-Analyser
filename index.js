const fs = require('fs');
const csv = require('csv-parser');
const filePath = 'CPI Timeseries.csv';


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
    // For simplicity, transform data by selecting a subset of columns and filtering by year (2020 to 2015)
    const transformedData = data
        //.filter((row) => row.Year >= 2015 && row.Year <= 2020)
        .map((row) => {
            return {
                Country: row.Country,
                'CPI score 2020': row['CPI score 2020'],
                'CPI score 2019': row['CPI score 2019'],
                'CPI score 2018': row['CPI score 2018'],
                'CPI score 2017': row['CPI score 2017'],
                'CPI score 2016': row['CPI score 2016'],
                'CPI score 2015': row['CPI score 2015'],
                'CPI score 2014': row['CPI score 2014'],
                'CPI score 2013': row['CPI score 2013'],
                'CPI score 2012': row['CPI score 2012']
            };
        });

    return transformedData;
}

async function runDataPipeline(filePath) {
    try {
        // Step 1: Extract Data
        const rawData = await extractData(filePath);
        //console.log(rawData);

        // Step 2: Transform Data
        const transformedData = transformData(rawData);
        console.log(transformedData);

        // Step 3: Load Data
        //loadData(transformedData);
    } catch (error) {
        console.error('Error in data pipeline:', error);
    }
}

const axios = require('axios');

const worldBankApiUrl = 'http://api.worldbank.org/V2';
const worldBankIndicators = [ 'ER.FSH.PROD.MT',   // Total fisheries production (metric tons)
'AG.SRF.TOTL.K2',   // Agricultural land (sq. km)
'IC.REG.DURS',      // Time required to start a business (days)
'IC.BUS.NREG',      // New businesses registered (number)
'SL.AGR.EMPL.ZS',   // Employment in agriculture (% of total employment) (modelled ILO estimate)
'SL.EMP.SELF.ZS'    // Self-employed, total (% of total employment) (modelled ILO estimate)
]; // Add more indicators as needed

// Step 1: Extract Data from World Bank API
async function extractWorldBankData() {
    try {
        const responses = (await axios.get(`${worldBankApiUrl}/country/CHN;TCD/indicator/${worldBankIndicators.join(';')}?format=json&date=2017:2022&source=2`)).data[1];
        console.log(responses);

        const data = responses.reduce((result, response, index) => {
            console.log(result, response, index);
            const indicator = worldBankIndicators[index];

           /* response.forEach((entry) => {
                result.push({
                    Country: entry.country.value,
                    Indicator: indicator,
                    Value: entry.value || null,
                });
            });*/

            return result;
        }, []);

        return data;
    } catch (error) {
        throw error;
    }
}

//runDataPipeline(filePath);
extractWorldBankData();