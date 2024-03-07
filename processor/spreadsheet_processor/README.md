SPREADSHEET PROCESSOR
---------------------

Description
------------
Spreadsheet processor fetch data from Google spreadsheets using Google API


Configuration
-------------
Available configuration parameters:

* **TARGET_TABLE** – Name of the table in the database where received data from Google sheets stored.

* **GOOGLE_CREDS** – JSON string with the service account key required for authorization through the service account to make a request to the Google Sheets API.

* **SCOPE** - Scopes to request during the authorization grant.

* **SPREADSHEET_ID** – The retrieval spreadsheet ID that initializes the connection to the desired table to retrieve data from.
  (For example, link to specific sheet is formed as follows: https://docs.google.com/spreadsheets/d/SPREADSHEET_ID)

* **RANGE** - The range from which values are retrieved. We can specify both a range of cells and a table sheet name.
