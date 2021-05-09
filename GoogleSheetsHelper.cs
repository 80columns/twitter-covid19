using Google.Apis.Auth.OAuth2;
using Google.Apis.Services;
using Google.Apis.Sheets.v4;
using Google.Apis.Sheets.v4.Data;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Threading;

namespace twitter {
    // code referenced from https://www.hardworkingnerd.com/how-to-read-and-write-to-google-sheets-with-c/
    public class GoogleSheetsHelper {
        private static string[] apiScopes = { SheetsService.Scope.Spreadsheets };
        private static string applicationName = Environment.GetEnvironmentVariable("GOOGLE_SHEETS_API_PROJECT_NAME");

        private readonly SheetsService sheetsService;
        private readonly string spreadsheetId;
        private readonly int sheetId;
        private readonly int maxRequestCountPerMinute = 60;
        private int currentRequestCount;

        public GoogleSheetsHelper(string _spreadSheetId, string _spreadSheetName) {
            var jsonCredential = JsonConvert.SerializeObject(
                new Dictionary<string, string>() {
                    ["type"] = Environment.GetEnvironmentVariable("GOOGLE_SHEETS_API_type"),
                    ["project_id"] = Environment.GetEnvironmentVariable("GOOGLE_SHEETS_API_project_id"),
                    ["private_key_id"] = Environment.GetEnvironmentVariable("GOOGLE_SHEETS_API_private_key_id"),
                    ["private_key"] = Environment.GetEnvironmentVariable("GOOGLE_SHEETS_API_private_key"),
                    ["client_email"] = Environment.GetEnvironmentVariable("GOOGLE_SHEETS_API_client_email"),
                    ["client_id"] = Environment.GetEnvironmentVariable("GOOGLE_SHEETS_API_client_id"),
                    ["auth_uri"] = Environment.GetEnvironmentVariable("GOOGLE_SHEETS_API_auth_uri"),
                    ["token_uri"] = Environment.GetEnvironmentVariable("GOOGLE_SHEETS_API_token_uri"),
                    ["auth_provider_x509_cert_url"] = Environment.GetEnvironmentVariable("GOOGLE_SHEETS_API_auth_provider_x509_cert_url"),
                    ["client_x509_cert_url"] = Environment.GetEnvironmentVariable("GOOGLE_SHEETS_API_client_x509_cert_url")
                }
            );

            var credential = GoogleCredential.FromJson(jsonCredential).CreateScoped(apiScopes);

            this.sheetsService = new SheetsService(
                new BaseClientService.Initializer() {
                    HttpClientInitializer = credential,
                    ApplicationName = applicationName,
                }
            );

            this.spreadsheetId = _spreadSheetId;
            this.sheetId = GetSheetId(_spreadSheetId, _spreadSheetName);

            this.currentRequestCount = 0;
        }

        public List<ExpandoObject> GetDataFromSheet(GoogleSheetParameters _googleSheetParameters) {
            _googleSheetParameters = MakeGoogleSheetDataRangeColumnsZeroBased(_googleSheetParameters);
            var range = $"{_googleSheetParameters.SheetName}!{GetColumnName(_googleSheetParameters.RangeColumnStart)}{_googleSheetParameters.RangeRowStart}:{GetColumnName(_googleSheetParameters.RangeColumnEnd)}{_googleSheetParameters.RangeRowEnd}";

            var request = sheetsService.Spreadsheets.Values.Get(spreadsheetId, range);

            var numberOfColumns = _googleSheetParameters.RangeColumnEnd - _googleSheetParameters.RangeColumnStart;
            var columnNames = new List<string>();
            var returnValues = new List<ExpandoObject>();

            if (!_googleSheetParameters.FirstRowIsHeaders) {
                for (var i = 0; i <= numberOfColumns; i++) {
                    columnNames.Add($"Column{i}");
                }
            }

            var response = request.Execute();

            int rowCounter = 0;
            var values = response.Values;

            if (values != null && values.Count > 0) {
                foreach (var row in values) {
                    if (_googleSheetParameters.FirstRowIsHeaders && rowCounter == 0) {
                        for (var i = 0; i <= numberOfColumns; i++) {
                            columnNames.Add(row[i].ToString());
                        }

                        rowCounter++;
                        continue;
                    }

                    var expando = new ExpandoObject();
                    var expandoDict = expando as IDictionary<String, object>;
                    var columnCounter = 0;

                    foreach (var columnName in columnNames) {
                        expandoDict.Add(columnName, row[columnCounter].ToString());
                        columnCounter++;
                    }

                    returnValues.Add(expando);
                    rowCounter++;
                }
            }

            return returnValues;
        }

        public HashSet<string> GetAllColumnValues(string _sheetName, string _columnName) {
            var columnValues = new HashSet<string>();

            var range = $"{_sheetName}!{_columnName}:{_columnName}";
            var request = sheetsService.Spreadsheets.Values.Get(spreadsheetId, range);

            var response = request.Execute();

            if (response.Values != null && response.Values.Count > 0) {
                foreach (var cell in response.Values) {
                    columnValues.Add(Convert.ToString(cell[0]));
                }
            }

            return columnValues;
        }

        public void AppendRows(List<GoogleSheetRow> _rows, ILogger _logger) {
            if (this.currentRequestCount + _rows.Count >= this.maxRequestCountPerMinute) {
                PauseGoogleApiCalls(_logger, 1_000 * 60);
            }

            var appended = false;

            while (appended == false) {
                try {
                    var requests = new BatchUpdateSpreadsheetRequest { Requests = new List<Request>() };

                    var request = new Request {
                        AppendCells = new AppendCellsRequest {
                            Fields = "*",
                            SheetId = this.sheetId
                        }
                    };

                    var listRowData = new List<RowData>();

                    foreach (var row in _rows) {
                        var rowData = new RowData();
                        var listCellData = new List<CellData>();

                        foreach (var cell in row.Cells) {
                            var cellData = new CellData();
                            var extendedValue = new ExtendedValue { StringValue = cell.CellValue };

                            cellData.UserEnteredValue = extendedValue;
                            var cellFormat = new CellFormat { TextFormat = new TextFormat(), WrapStrategy = "Wrap" };

                            if (cell.IsBold) {
                                cellFormat.TextFormat.Bold = true;
                            }

                            cellFormat.BackgroundColor = new Color {
                                Blue = (float)cell.BackgroundColor.B / 255,
                                Red = (float)cell.BackgroundColor.R / 255,
                                Green = (float)cell.BackgroundColor.G / 255
                            };

                            cellData.UserEnteredFormat = cellFormat;
                            listCellData.Add(cellData);
                        }

                        rowData.Values = listCellData;
                        listRowData.Add(rowData);
                    }

                    request.AppendCells.Rows = listRowData;

                    // It's a batch request so you can create more than one request and send them all in one batch. Just use reqs.Requests.Add() to add additional requests for the same spreadsheet
                    requests.Requests.Add(request);

                    // sleep 200 ms to add delay to requests
                    Thread.Sleep(200);

                    sheetsService.Spreadsheets.BatchUpdate(requests, spreadsheetId).Execute();

                    this.currentRequestCount += _rows.Count;

                    appended = true;
                } catch (Exception e) {
                    _logger.LogInformation($"caught exception when writing to google spreadsheet, message is {e.Message}, exception type is {e.GetType()}");

                    PauseGoogleApiCalls(_logger, 60 * 1_000);
                }
            }

            
        }

        private void PauseGoogleApiCalls(ILogger _logger, int _millisecondsTimeout) {
            _logger.LogInformation($"current google sheets request count is {this.currentRequestCount} and max request count is {this.maxRequestCountPerMinute}, sleeping for {_millisecondsTimeout/1000} seconds...");

            // sleep for 1 minute before making more requests
            Thread.Sleep(_millisecondsTimeout);

            this.currentRequestCount = 0;

            _logger.LogInformation($"finished sleeping for 1 minute");
        }

        private string GetColumnName(int _index) {
            const string letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
            var value = "";

            if (_index >= letters.Length) {
                value += letters[_index / letters.Length - 1];
            }

            value += letters[_index % letters.Length];

            return value;
        }

        private GoogleSheetParameters MakeGoogleSheetDataRangeColumnsZeroBased(GoogleSheetParameters _googleSheetParameters) {
            _googleSheetParameters.RangeColumnStart = _googleSheetParameters.RangeColumnStart - 1;
            _googleSheetParameters.RangeColumnEnd = _googleSheetParameters.RangeColumnEnd - 1;

            return _googleSheetParameters;
        }

        private int GetSheetId(string _spreadSheetId, string _spreadSheetName) {
            var spreadsheet = this.sheetsService.Spreadsheets.Get(_spreadSheetId).Execute();
            var sheet = spreadsheet.Sheets.FirstOrDefault(s => s.Properties.Title == _spreadSheetName);
            var sheetId = (int)sheet.Properties.SheetId;

            return sheetId;
        }
    }

    public class GoogleSheetCell {
        public string CellValue { get; set; }
        public bool IsBold { get; set; }
        public System.Drawing.Color BackgroundColor { get; set; } = System.Drawing.Color.White;
    }

    public class GoogleSheetParameters {
        public int RangeColumnStart { get; set; }
        public int RangeRowStart { get; set; }
        public int RangeColumnEnd { get; set; }
        public int RangeRowEnd { get; set; }
        public string SheetName { get; set; }
        public bool FirstRowIsHeaders { get; set; }
    }

    public class GoogleSheetRow {
        public GoogleSheetRow() => Cells = new List<GoogleSheetCell>();

        public List<GoogleSheetCell> Cells { get; set; }
    }
}
