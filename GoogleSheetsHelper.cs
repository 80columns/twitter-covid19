using Google.Apis.Auth.OAuth2;
using Google.Apis.Services;
using Google.Apis.Sheets.v4;
using Google.Apis.Sheets.v4.Data;
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
        private readonly int maxRequestCountPerMinute = 60;
        private int currentRequestCount;

        public GoogleSheetsHelper(string spreadsheetId) {
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

            sheetsService = new SheetsService(
                new BaseClientService.Initializer() {
                    HttpClientInitializer = credential,
                    ApplicationName = applicationName,
                }
            );

            this.spreadsheetId = spreadsheetId;

            this.currentRequestCount = 0;
        }

        public List<ExpandoObject> GetDataFromSheet(GoogleSheetParameters googleSheetParameters) {
            googleSheetParameters = MakeGoogleSheetDataRangeColumnsZeroBased(googleSheetParameters);
            var range = $"{googleSheetParameters.SheetName}!{GetColumnName(googleSheetParameters.RangeColumnStart)}{googleSheetParameters.RangeRowStart}:{GetColumnName(googleSheetParameters.RangeColumnEnd)}{googleSheetParameters.RangeRowEnd}";

            var request = sheetsService.Spreadsheets.Values.Get(spreadsheetId, range);

            var numberOfColumns = googleSheetParameters.RangeColumnEnd - googleSheetParameters.RangeColumnStart;
            var columnNames = new List<string>();
            var returnValues = new List<ExpandoObject>();

            if (!googleSheetParameters.FirstRowIsHeaders) {
                for (var i = 0; i <= numberOfColumns; i++) {
                    columnNames.Add($"Column{i}");
                }
            }

            var response = request.Execute();

            int rowCounter = 0;
            var values = response.Values;

            if (values != null && values.Count > 0) {
                foreach (var row in values) {
                    if (googleSheetParameters.FirstRowIsHeaders && rowCounter == 0) {
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

        public void AppendRows(List<GoogleSheetRow> rows, string sheetName) {
            if (this.currentRequestCount >= this.maxRequestCountPerMinute) {
                // sleep for 1 minute before making more requests
                Thread.Sleep(1_000 * 60);

                this.currentRequestCount = 0;
            }

            var requests = new BatchUpdateSpreadsheetRequest { Requests = new List<Request>() };
            var sheetId = GetSheetId(sheetsService, spreadsheetId, sheetName);

            var request = new Request {
                AppendCells = new AppendCellsRequest {
                    Fields = "*",
                    SheetId = sheetId
                }
            };

            var listRowData = new List<RowData>();

            foreach (var row in rows) {
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

            this.currentRequestCount++;
        }

        private string GetColumnName(int index) {
            const string letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
            var value = "";

            if (index >= letters.Length) {
                value += letters[index / letters.Length - 1];
            }

            value += letters[index % letters.Length];

            return value;
        }

        private GoogleSheetParameters MakeGoogleSheetDataRangeColumnsZeroBased(GoogleSheetParameters googleSheetParameters) {
            googleSheetParameters.RangeColumnStart = googleSheetParameters.RangeColumnStart - 1;
            googleSheetParameters.RangeColumnEnd = googleSheetParameters.RangeColumnEnd - 1;

            return googleSheetParameters;
        }

        private int GetSheetId(SheetsService service, string spreadSheetId, string spreadSheetName) {
            var spreadsheet = service.Spreadsheets.Get(spreadSheetId).Execute();
            var sheet = spreadsheet.Sheets.FirstOrDefault(s => s.Properties.Title == spreadSheetName);
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
