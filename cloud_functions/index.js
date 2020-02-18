// Copyright 2018 Google LLC.
// SPDX-License-Identifier: Apache-2.0

const { google } = require("googleapis");
const { Storage } = require("@google-cloud/storage");

exports.csv2sheet = async (data, context) => {
  var fileName = data.name;
  // basic check that this is a *.csv file
  if (!fileName.endsWith(".csv")) {
    console.log("Not a .csv file, ignoring.");
    return;
  }
  // define name of new sheet
  const sheetName = fileName.slice(0, -4);
  
  // block on auth + getting the sheets API object
  const auth = await google.auth.getClient({
    scopes: [
      "https://www.googleapis.com/auth/spreadsheets",
      "https://www.googleapis.com/auth/devstorage.read_only"
    ]
  });
  const sheetsAPI = google.sheets({ version: "v4", auth });
  
  // get sheet or create a new sheet and remember its ID (based on the filename, removing the .csv extension)
  const sheetId = await getSheetId(sheetsAPI, sheetName);
  const theData = await readCSVContent(sheetsAPI, data, sheetName);
  await populateAndStyle(sheetsAPI, theData, sheetId);
};

function getSheetId(sheetsAPI, sheetName) {
  return new Promise((resolve, reject) => {
    const getSheetIdParams = {
      spreadsheetId: process.env.SPREADSHEET_ID,
      ranges: sheetName,
      fields: "sheets/properties(sheetId)"
    };
    
    sheetsAPI.spreadsheets.get(getSheetIdParams, function(
      err,
      response
    ) {
      if (err) {
        console.log("Sheets API returned an error while getting " + sheetName + " sheetId: " + err);
        const sheetId = addEmptySheet(sheetsAPI, sheetName);
        resolve(sheetId);
        //reject("Sheets API returned an error while getting " + sheetName + " sheetsId: " + err);
      } else {
        const sheetId = response.data.sheets[0].properties.sheetId;
        console.log("Found " + sheetName + " sheetId: " + sheetId);
        resolve(sheetId);
      }
    });
  });
}

// Creates a new sheet in the spreadsheet with the given name at position 2,
// with 26 colums and 2000 rows with the first row frozen.
// Returns its sheetId
function addEmptySheet(sheetsAPI, sheetName) {
  return new Promise((resolve, reject) => {
    const addEmptySheetParams = {
      // reading SHEET_ID from function environment variable
      spreadsheetId: process.env.SPREADSHEET_ID,
      resource: {
        requests: [
          {
            addSheet: {
              properties: {
                title: sheetName,
                index: 1,
                gridProperties: {
                  rowCount: 20000,
                  columnCount: 26,
                  frozenRowCount: 1
                }
              }
            }
          }
        ]
      }
    };
    
    sheetsAPI.spreadsheets.batchUpdate(addEmptySheetParams, function(
      err,
      response
    ) {
      if (err) {
        reject("Sheets API returned an error while adding empty sheet: " + err);
      } else {
        const sheetId = response.data.replies[0].addSheet.properties.sheetId;
        console.log("Created empty sheet: " + sheetId);
        resolve(sheetId);
      }
    });
  });
}

// read data from the CSV file uploaded to the storage bucket
// and returns a string of CSV values with carriage returns
function readCSVContent(sheetsAPI, file, sheetName) {
  return new Promise((resolve, reject) => {
    const storage = new Storage();
    let fileContents = new Buffer('');
    storage.bucket(file.bucket).file(file.name).createReadStream()
    .on('error', function(err) {
      reject('Storage API returned an error: ' + err);
    })
    .on('data', function(chunk) {
      fileContents = Buffer.concat([fileContents, chunk]);
    })  
    .on('end', function() {
      let content = fileContents.toString('utf8');
      console.log("CSV content read as string: " + content );
      resolve(content);
    });
  });
}

function populateAndStyle(sheetsAPI, theData, sheetId) {
  return new Promise((resolve, reject) => {
    // Using 'batchUpdate' allows for multiple 'requests' to be sent in a single batch.
    // Populate the sheet referenced by its ID with the data received (a CSV string)
    // Style: set first row font size to 11 and to Bold. Exercise left for the reader: resize columns
    const dataAndStyle = {
      spreadsheetId: process.env.SPREADSHEET_ID,
      resource: {
        requests: [
          {
            pasteData: {
              coordinate: {
                sheetId: sheetId,
                rowIndex: 0,
                columnIndex: 0
              },
              data: theData,
              delimiter: ","
            }
          },
          {
            repeatCell: {
              range: {
                sheetId: sheetId,
                startRowIndex: 0,
                endRowIndex: 1
              },
              cell: {
                userEnteredFormat: {
                  textFormat: {
                    fontSize: 11,
                    bold: true
                  }
                }
              },
              fields: "userEnteredFormat(textFormat)"
            }
          }
        ]
      }
    };
    
    sheetsAPI.spreadsheets.batchUpdate(dataAndStyle, function(err, response) {
      if (err) {
        reject("Sheets API returned an error while populating: " + err);
      } else {
        console.log(sheetId + " sheet populated with " + theData.length + " rows and column style set.");
        resolve();
      }
    });
  });
}