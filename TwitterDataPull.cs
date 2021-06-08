using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using twitter;

namespace cs_covid19_data_pull {
    public class TwitterSearchResult {
        public List<TwitterPost> data;
        public SearchMetadata meta;
    }

    public class TwitterPost {
        public string id;
        public string text;
        public string created_at;
    }

    public class SearchMetadata {
        public string newest_id;
        public string oldest_id;
        public string result_count;
        public string next_token;
    }

    public static class TwitterDataPull {
        // script resources
        private static ILogger logger;
        private static HttpClient httpClient = new();

        private static string[] twitterLocationTerms;
        private static string[] twitterResourceTerms;
        private static string[] twitterPhoneTerms;

        // separate exclusion terms here due to twitter length restriction on query string
        private static readonly string[] localExclusionStrings = new string[] {
            "need",
            "require",
            "patient",
            "request for",
            "urgent help",
            "please help",
            "plz help",
            "pls help",
            "bot link",
            "relative",
            "symptoms",
            "help please",
            "can you help",
            "admitted to",
            "condition",
            "urgently looking",
            "any potential",
            "looking for a",
            "my close friend",
            "currently hospitalised",
            "help with an",
            "#urgenthelp",
            "pellucid",
            "sos call",
            "please see and help",
            "is looking for",
            "is suffering from",
            "is admitted in",
            "is struggling",
            "dream home",
            "is severely affected",
            "they are looking for",
            "admission date",
            "luxury apartments",
            "plese help"
        };
        private static readonly Dictionary<string, string[]> resourceAdditionalDetails = new() {
            ["plasma"] = new string[] { "[^a-z]a\\+", "[^a-z]a\\-", "[^a-z]b\\+", "[^a-z]b\\-", "[^a-z]o\\+", "[^a-z]o\\-", "[^a-z]ab\\+", "[^a-z]ab\\-" },
            ["oxygen"] = new string[] { "can", "cylinder", "concentrator", "refill" }
        };

        // twitter post phone number regex
        private static readonly string phoneNumberPattern = @"([\t -]*?[0-9][\t -]*?){10,}";

        // google sheets helper with spreadsheet id
        private static GoogleSheetsHelper googleSheet = new(Environment.GetEnvironmentVariable("GOOGLE_SHEET_ID"), "Sheet1");

        // azure blob client
        private static BlobServiceClient blobServiceClient = new(Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING"));

        // list of historical unique phone numbers
        private static Dictionary<string, DateTimeOffset> historicalPhoneNumbers;
        private static Dictionary<string, DateTimeOffset> currentRunPhoneNumbers = new();

        private static bool usingPresetData = false;

        // run this function once every two hours at the start of the hour, e.g. 12:00 am, 2:00 am, 4:00 am etc.
        // 0 0 */2 * * *
        // 0 */1 * * * *
        // see https://docs.microsoft.com/en-us/azure/azure-functions/functions-bindings-timer?tabs=csharp for explanation of cron format
        [Function("TwitterDataPull")]
        public static async Task Run(
            [TimerTrigger("0 0 */2 * * *" // 2-hour script trigger time for automatic processing
                #if DEBUG
                , RunOnStartup=true // https://stackoverflow.com/a/51775445
                #endif
            )] MyInfo myTimer, FunctionContext context) {

            #if DEBUG
                // force-launch visual studio debugger in debug mode, otherwise breakpoints will not be caught (issue with .NET 5 debugging in az functions)
                Debugger.Launch();
            #endif

            logger = context.GetLogger("TwitterDataPull");

            try {
                logger.LogInformation($"Script started at: {DateTimeOffset.UtcNow} UTC");
                logger.LogInformation($"Next timer schedule at: {myTimer.ScheduleStatus.Next.ToUniversalTime()} UTC");

                twitterLocationTerms = (await googleSheet.GetAllColumnValuesAsync("Sheet2", "A", _ignoreHeader: true)).Select(city => city.Contains(' ') ? $"\"{city}\"" : city).ToArray();
                twitterResourceTerms = (await googleSheet.GetAllColumnValuesAsync("Sheet2", "B", _ignoreHeader: true)).Select(resource => resource.Contains(' ') ? $"\"{resource}\"" : resource).ToArray();
                twitterPhoneTerms = (await googleSheet.GetAllColumnValuesAsync("Sheet2", "C", _ignoreHeader: true)).Select(phoneTerm => phoneTerm.Contains(' ') ? $"\"{phoneTerm}\"" : phoneTerm).ToArray();

                // get the prior phone numbers pulled by the script
                await LoadUniquePhoneNumbersAsync();

                logger.LogInformation($"pulled {historicalPhoneNumbers.Count} historical phone numbers");

                #if DEBUG
                    var twitterPosts = usingPresetData ? await ReadLocalV2Data() : await FetchTwitterV2DataAsync(_postTimeRange: TimeSpan.FromHours(2));

                    if (usingPresetData == false) {
                        await SaveLocalData(twitterPosts);
                    }
                #else
                    var twitterPosts = usingPresetData ? await ReadStorageAccountV2Data() : await FetchTwitterV2DataAsync(_postTimeRange: TimeSpan.FromHours(2));
                #endif

                logger.LogInformation($"processing {twitterPosts.Count} posts for extracting phone numbers");

                LogData(twitterPosts);

                await SaveUniquePhoneNumbersAsync();

                AppendCompletionSpreadsheetRow();

                logger.LogInformation($"Script completed at {DateTimeOffset.UtcNow} UTC");
            } catch (Exception e) {
                #if DEBUG
                    logger.LogInformation($"caught exception in top-level function, message is '{e.Message}' and type is {e.GetType()}");
                #else
                    logger.LogInformation($"caught exception in top-level function, message is '{e.Message}' and type is {e.GetType()}");
                    throw;
                #endif
            }
        }

        private static async Task ResetPhoneNumbersAsync() {
            var lines = await File.ReadAllLinesAsync(@"C:\Users\patri\OneDrive\Desktop\historicalPhoneNumbers.txt");
            historicalPhoneNumbers = new Dictionary<string, DateTimeOffset>();

            foreach (var line in lines) {
                var lineParts = line.Split('\t', StringSplitOptions.RemoveEmptyEntries);

                if (lineParts.Length == 2) {
                    if (historicalPhoneNumbers.ContainsKey(lineParts[0].Trim()) == false) {
                        historicalPhoneNumbers.Add(lineParts[0].Trim(), DateTimeOffset.Parse(lineParts[1].Trim()));
                    }
                } else {
                    logger.LogInformation($"invalid line detected");
                }
            }

            await SaveUniquePhoneNumbersAsync();
        }

        public static async Task LoadUniquePhoneNumbersAsync() {
            var containerClient = blobServiceClient.GetBlobContainerClient(Environment.GetEnvironmentVariable("PHONE_NUMBER_BLOB_CONTAINER"));
            var blobClient = containerClient.GetBlobClient(Environment.GetEnvironmentVariable("PHONE_NUMBER_JSON_FILE"));

            using (var destinationStream = new MemoryStream()) {
                await blobClient.DownloadToAsync(destinationStream);

                var destinationString = Encoding.UTF8.GetString(destinationStream.ToArray());

                historicalPhoneNumbers = JsonConvert.DeserializeObject<Dictionary<string, DateTimeOffset>>(destinationString);
            }
        }

        public static async Task SaveUniquePhoneNumbersAsync() {
            var containerClient = blobServiceClient.GetBlobContainerClient(Environment.GetEnvironmentVariable("PHONE_NUMBER_BLOB_CONTAINER"));
            var blobClient = containerClient.GetBlobClient(Environment.GetEnvironmentVariable("PHONE_NUMBER_JSON_FILE"));
            var combinedHistoricalPhoneNumbers = historicalPhoneNumbers.Union(currentRunPhoneNumbers).ToDictionary(item => item.Key, item => item.Value);

            logger.LogInformation($"saved unique phone numbers, count is now {combinedHistoricalPhoneNumbers.Count}");

            var sourceString = JsonConvert.SerializeObject(combinedHistoricalPhoneNumbers);

            await blobClient.UploadAsync(new MemoryStream(Encoding.UTF8.GetBytes(sourceString)), overwrite: true);
        }

        public static async Task<List<TwitterPost>> FetchTwitterV2DataAsync(TimeSpan _postTimeRange) {
            var twitterPosts = new List<TwitterPost>();

            // find the tweets with the terms in phoneTerms + resourceTerms
            var twitterSearchResult = null as TwitterSearchResult;
            var startTime = DateTimeOffset.UtcNow.Subtract(_postTimeRange);
            var startTimeString = HttpUtility.UrlEncode(startTime.ToString("yyyy-MM-ddTHH:mm:ssZ"));
            var tweetFields = string.Join(",", new string[] { "id", "text", "created_at" });
            var iteration = 0;
            var maxResults = 100;

            // search for 15 locations at a time to reduce the number of api calls below
            // the total # of api calls below needs to be < 450 as the twitter api limits us to 450 queries per 15 minutes,
            // and azure functions consumption plan limits function runtime to 10 minutes so when running in azure, we can't actually wait 15 min and retry after an http 429 response
            var locationTermLimit = 15;

            for (var i = 0; i < twitterLocationTerms.Length; i += locationTermLimit) {
                locationTermLimit = (i + locationTermLimit > twitterLocationTerms.Length) ? twitterLocationTerms.Length - i : locationTermLimit;

                for (var j = 0; j < twitterResourceTerms.Length; j++) {
                    var queryString = HttpUtility.UrlEncode(
                        "-is:retweet"
                     + $" ({string.Join(" OR ", twitterLocationTerms[i..(i + locationTermLimit)])})"
                     + $" {twitterResourceTerms[j]}"
                     + $" ({string.Join(" OR ", twitterPhoneTerms)})"
                    );

                    do {
                        var requestUri = $"https://api.twitter.com/2/tweets/search/recent?start_time={startTimeString}&query={queryString}&tweet.fields={tweetFields}&max_results={maxResults}"
                                         + (string.IsNullOrWhiteSpace(twitterSearchResult?.meta?.next_token) ? string.Empty : $"&next_token={twitterSearchResult?.meta?.next_token}");

                        using (var request = new HttpRequestMessage(HttpMethod.Get, requestUri)) {
                            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", Environment.GetEnvironmentVariable("TWITTER_API_OAUTH_TOKEN"));

                            using (var response = await httpClient.SendAsync(request)) {
                                if (response.StatusCode == HttpStatusCode.TooManyRequests) {
                                    PauseTwitterApiQueries(iteration);

                                    // decrement j to retry the same request again after pausing above
                                    j--;
                                } else {
                                    var responseString = await response.Content.ReadAsStringAsync();
                                    twitterSearchResult = JsonConvert.DeserializeObject<TwitterSearchResult>(responseString);

                                    if (
                                        twitterSearchResult?.data != null
                                     && twitterSearchResult.data.Any()
                                    ) {
                                        twitterPosts.AddRange(
                                            twitterSearchResult.data.Where(post => twitterPosts.All(currentPost => currentPost.id != post.id))
                                                                    .Where(post => ValidatePhoneNumber(post))
                                                                    .Where(post => CheckExclusionStrings(post))
                                        );
                                    } else {
                                        logger.LogInformation($"no results returned when looking for posts with locations {string.Join('/', twitterLocationTerms[i..(i + locationTermLimit)])} and resource {twitterResourceTerms[j]} after {startTime} UTC");
                                    }

                                    // sleep 300 ms to add delay to the requests
                                    Thread.Sleep(300);

                                    logger.LogInformation($"got {twitterSearchResult.meta.result_count} posts from twitter api on iteration {iteration}");

                                    iteration++;

                                    if ((iteration + 1) % 450 == 0) {
                                        PauseTwitterApiQueries(iteration);
                                    }
                                }
                            }
                        }
                    } while (twitterSearchResult?.meta?.next_token != null);
                }
            }

            logger.LogInformation($"got {twitterPosts.Count} total matching posts from twitter v2 api after {iteration} iterations");

            return twitterPosts;
        }

        public static async Task<List<TwitterPost>> FetchTwitterV11DataAsync(TimeSpan _postTimeRange, string _startTimeString = null, string _nextToken = null) {
            var twitterPosts = new List<TwitterPost>();
            var startTime = DateTimeOffset.UtcNow.Subtract(_postTimeRange);
            var startTimeString = _startTimeString ?? HttpUtility.UrlEncode(startTime.ToString("yyyyMMddHHmm"));
            var iteration = 0;
            var maxResults = 500;
            var nextToken = _nextToken ?? string.Empty;
            var locationLimit = 30;

            // query using 30 locations at a time so the query string isn't longer than 1024 characters
            for (var i = 0; i < twitterLocationTerms.Length; i += locationLimit) {
                locationLimit = (i + locationLimit > twitterLocationTerms.Length) ? twitterLocationTerms.Length - i : locationLimit;

                var currentTwitterLocationTerms = twitterLocationTerms.ToList().GetRange(i, locationLimit);

                var queryString = HttpUtility.UrlEncode(
                    "-is:retweet"
                 + $" ({string.Join(" OR ", currentTwitterLocationTerms)})"
                 + $" ({string.Join(" OR ", twitterResourceTerms)})"
                 + $" ({string.Join(" OR ", twitterPhoneTerms)})"
                );

                do {
                    var requestUri = $"https://api.twitter.com/1.1/tweets/search/30day/prod.json?fromDate={startTimeString}&query={queryString}&maxResults={maxResults}"
                                     + (string.IsNullOrWhiteSpace(nextToken) ? string.Empty : $"&next={HttpUtility.UrlEncode(nextToken)}");

                    using (var request = new HttpRequestMessage(HttpMethod.Get, requestUri)) {
                        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", Environment.GetEnvironmentVariable("TWITTER_API_OAUTH_TOKEN"));

                        var retryRequest = true;

                        while (retryRequest) {
                            using (var response = await httpClient.SendAsync(request)) {
                                if (response.StatusCode == HttpStatusCode.TooManyRequests) {
                                    PauseTwitterApiQueries(iteration);
                                } else {
                                    var responseString = await response.Content.ReadAsStringAsync();
                                    var responseJson = JObject.Parse(responseString);

                                    twitterPosts.AddRange(
                                        ParseV11Data(responseJson).Where(post => ValidatePhoneNumber(post))
                                                                  .Where(post => CheckExclusionStrings(post))
                                    );

                                    nextToken = responseJson.ContainsKey("next") ? Convert.ToString(responseJson["next"]) : string.Empty;

                                    // sleep 200 ms to add delay to the requests
                                    Thread.Sleep(200);

                                    logger.LogInformation($"got {responseJson["results"].Count()} posts from twitter api on iteration {iteration}");

                                    iteration++;
                                    retryRequest = false;
                                }
                            }
                        }
                    }
                } while (string.IsNullOrWhiteSpace(nextToken) == false);
            }

            logger.LogInformation($"got {twitterPosts.Count} total matching posts from twitter v1.1 api after {iteration} iterations");

            return twitterPosts;
        }

        public static void PauseTwitterApiQueries(int _iteration) {
            logger.LogInformation($"sleeping for 15 minutes, {_iteration} requests have been made so far to twitter api");

            // if 450 requests have been made, sleep for 15 minutes until the next batch of requests can start
            // 1_000 ms (1 sec) * 60 sec/min * 15 min = 900 sec
            Thread.Sleep(1_000 * 60 * 15);

            logger.LogInformation($"finished sleeping for 15 minutes, resuming data pull");
        }

        public static async Task<List<TwitterPost>> ReadLocalV2Data() {
            var data = await File.ReadAllTextAsync(@"C:\Users\patri\OneDrive\Desktop\CaregiverSaathiCovid19\cs-covid19-data-pull\repo\sampleDataV2.json");

            return JsonConvert.DeserializeObject<List<TwitterPost>>(data);
        }

        public static async Task<List<TwitterPost>> ReadLocalV11Data() {
            var data = await File.ReadAllTextAsync(@"C:\Users\patri\OneDrive\Desktop\CaregiverSaathiCovid19\cs-covid19-data-pull\repo\sampleDataV11.json");
            var jsonData = JObject.Parse(data);
            var twitterPosts = ParseV11Data(jsonData);

            return twitterPosts.Where(post => ValidatePhoneNumber(post))
                               .Where(post => CheckExclusionStrings(post))
                               .ToList();
        }

        public static async Task<List<TwitterPost>> ReadStorageAccountV2Data() {
            var containerClient = blobServiceClient.GetBlobContainerClient("test-data");
            var blobClient = containerClient.GetBlobClient("sampleDataV2.json");

            using (var destinationStream = new MemoryStream()) {
                await blobClient.DownloadToAsync(destinationStream);

                var destinationString = Encoding.UTF8.GetString(destinationStream.ToArray());

                return JsonConvert.DeserializeObject<List<TwitterPost>>(destinationString);
            }
        }

        public static async Task<List<TwitterPost>> ReadStorageAccountV11Data() {
            var containerClient = blobServiceClient.GetBlobContainerClient("test-data");
            var blobClient = containerClient.GetBlobClient("sampleDataV11.json");

            using (var destinationStream = new MemoryStream()) {
                await blobClient.DownloadToAsync(destinationStream);

                var destinationString = Encoding.UTF8.GetString(destinationStream.ToArray());

                var twitterPosts = ParseV11Data(JObject.Parse(destinationString));

                return twitterPosts.Where(post => ValidatePhoneNumber(post))
                                   .Where(post => CheckExclusionStrings(post))
                                   .ToList();
            }
        }

        public static List<TwitterPost> ParseV11Data(JObject _jsonData) {
            var twitterPosts = new List<TwitterPost>();
            var results = _jsonData["results"];

            foreach (JObject result in results) {
                var text = result.ContainsKey("extended_tweet") ? Convert.ToString(result["extended_tweet"]["full_text"]) : Convert.ToString(result["text"]);
                var id = Convert.ToString(result["id"]);

                // parse date string from format "Sat May 08 20:35:27 +0000 2021" so it can be parsed to a DateTimeOffset later on
                var dateCreatedParts = Convert.ToString(result["created_at"]).Split(' ', StringSplitOptions.RemoveEmptyEntries);
                var dateCreated = $"{dateCreatedParts[5]} {dateCreatedParts[1]} {dateCreatedParts[2]} {dateCreatedParts[3]} {dateCreatedParts[4]}";

                twitterPosts.Add(
                    new TwitterPost() {
                        id = id,
                        text = text,
                        created_at = dateCreated
                    }
                );
            }

            return twitterPosts;
        }

        public static async Task SaveLocalData(List<TwitterPost> _twitterPosts) {
            var dataString = JsonConvert.SerializeObject(_twitterPosts);

            await File.WriteAllTextAsync(@"C:\Users\patri\OneDrive\Desktop\CaregiverSaathiCovid19\cs-covid19-data-pull\repo\sampleDataOutput.json", dataString);
        }

        public static void LogData(List<TwitterPost> _twitterPosts) {
            // record City/Resource/Text/Phone No./Date Tweeted
            var itemsLogged = 0;
            var postsProcessed = 0;

            foreach (var post in _twitterPosts) {
                var spreadsheetRows = new List<GoogleSheetRow>();
                var phoneNumberMatches = Regex.Matches(post.text, phoneNumberPattern);

                foreach (Match numberMatch in phoneNumberMatches) {
                    var numberMatchValue = numberMatch.Value.Trim(new char[] { ' ', '-' });
                    var numbers = new List<string>();

                    if (numberMatchValue.Length > 20 && numberMatchValue.Contains(' ')) {
                        numbers.AddRange(numberMatchValue.Split(' ', StringSplitOptions.RemoveEmptyEntries).Select(number => number.Replace("-", string.Empty)));
                    } else {
                        numbers.Add(numberMatchValue.Replace(" ", string.Empty).Replace("-", string.Empty));
                    }

                    for (var i = 0; i < numbers.Count; i++) {
                        var number = numbers[i];

                        if (number.Length == 11 && number[0] == '0') {
                            // handle cell phone numbers starting with 0
                            number = number.Substring(1, number.Length - 1);
                        } else if (number.Length == 12 && number.Substring(0, 2) == "91") {
                            // handle numbers starting with the country code +91
                            number = number.Substring(2, number.Length - 2);
                        }

                        var matchingLocations = twitterLocationTerms.Where(location => post.text.ToLower().Contains(location));
                        var matchingResources = twitterResourceTerms.Where(resource => post.text.ToLower().Contains(resource));
                        var resourceDetails = new List<string>();

                        foreach (var resource in matchingResources) {
                            if (resourceAdditionalDetails.ContainsKey(resource)) {
                                foreach (var resourceItem in resourceAdditionalDetails[resource]) {
                                    if (Regex.IsMatch(post.text.ToLower(), resourceItem)) {
                                        if (resource == "plasma") {
                                            resourceDetails.Add(CapitalizeWord(resourceItem[(resourceItem.IndexOf(']') + 1)..].Replace("\\", string.Empty)));
                                        } else {
                                            resourceDetails.Add(CapitalizeWord(resourceItem));
                                        }
                                    }
                                }
                            }
                        }

                        if (IsPriorPhoneNumber(number) == false) {
                            // save phone number for lookup later to ensure we don't process duplicate phone numbers across subsequent runs of this script
                            if (currentRunPhoneNumbers.ContainsKey(number) == false) {
                                currentRunPhoneNumbers.Add(number, DateTimeOffset.Parse(post.created_at));

                                // add row to the google spreadsheet
                                spreadsheetRows.Add(
                                    new GoogleSheetRow() {
                                        Cells = new List<GoogleSheetCell>() {
                                            new GoogleSheetCell() { CellValue = string.Join(", ", matchingLocations.Select(location => CapitalizeWord(location))) },
                                            new GoogleSheetCell() { CellValue = string.Join(", ", matchingResources.Select(resource => CapitalizeWord(resource))) },
                                            new GoogleSheetCell() { CellValue = string.Join(", ", resourceDetails) },
                                            new GoogleSheetCell() { CellValue = $"+91{number}" },
                                            new GoogleSheetCell() { CellValue = post.created_at },
                                            new GoogleSheetCell() { CellValue = post.text.Replace("\\n", "\n") },
                                        }
                                    }
                                );

                                logger.LogInformation($"id: {post.id}, locations: {string.Join(", ", matchingLocations)}, resources: {string.Join(", ", matchingResources)}, phone #: {number}, date tweeted: {post.created_at}");
                            } else {
                                logger.LogInformation($"duplicate phone number {number} found within single run, not adding to spreadsheet");
                            }
                        } else {
                            logger.LogInformation($"id: {post.id}, duplicate phone number {number} found, not adding to spreadsheet");
                        }
                    }
                }

                if (spreadsheetRows.Any()) {
                    googleSheet.AppendRows(spreadsheetRows, logger);
                    itemsLogged += spreadsheetRows.Count;

                    logger.LogInformation($"logged {spreadsheetRows.Count} items to output spreadsheet");
                }

                postsProcessed++;
                logger.LogInformation($"processed post {postsProcessed} / {_twitterPosts.Count}");
            }

            logger.LogInformation($"logged {itemsLogged} total items to output spreadsheet");
        }

        public static bool ValidatePhoneNumber(TwitterPost _post) {
            logger.LogInformation($"checking tweet {_post.id} for match");

            var phoneNumberMatch = Regex.IsMatch(_post.text, phoneNumberPattern);

            if (phoneNumberMatch) {
                logger.LogInformation($"tweet {_post.id} matched");
            }

            return phoneNumberMatch;
        }

        public static bool IsPriorPhoneNumber(string _phoneNumber) {
            return historicalPhoneNumbers.ContainsKey(_phoneNumber);
        }

        public static bool CheckExclusionStrings(TwitterPost _post) {
            return localExclusionStrings.Any(term => _post.text.ToLower().Contains(term)) == false;
        }

        public static void AppendCompletionSpreadsheetRow() {
            var row = new GoogleSheetRow() {
                Cells = new List<GoogleSheetCell>() {
                    new GoogleSheetCell() { CellValue = "script completed", IsBold = true },
                    new GoogleSheetCell() { CellValue = "script completed", IsBold = true },
                    new GoogleSheetCell() { CellValue = "script completed", IsBold = true },
                    new GoogleSheetCell() { CellValue = "script completed", IsBold = true },
                    new GoogleSheetCell() { CellValue = "script completed", IsBold = true },
                    new GoogleSheetCell() { CellValue = "script completed", IsBold = true },
                }
            };

            googleSheet.AppendRows(new List<GoogleSheetRow>() { row }, logger);
        }

        public static string CapitalizeWord(string _word) {
            if (_word.ToLower() == "icu") {
                return "ICU";
            } else if (_word.ToLower() == "ab+") {
                return "AB+";
            } else if (_word.ToLower() == "ab-") {
                return "AB-";
            } else {
                return _word.Length >= 2 ? _word.Substring(0, 1).ToUpper() + _word[1..] : _word.ToUpper();
            }
        }
    }

    // auto-generated class from azure functions
    public class MyInfo {
        public MyScheduleStatus ScheduleStatus { get; set; }

        public bool IsPastDue { get; set; }
    }

    // auto-generated class from azure functions
    public class MyScheduleStatus {
        public DateTime Last { get; set; }

        public DateTime Next { get; set; }

        public DateTime LastUpdated { get; set; }
    }
}
