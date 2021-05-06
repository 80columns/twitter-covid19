using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
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

        // twitter query search terms
        #region twitterSearchTerms
        private static readonly string[] phoneTerms = new string[] {
            "contact",
            "verified",
            "number",
            "numbers",
            "whatsapp",
            "\"whats app\"",
            "helpline",
            "call"
        };
        private static readonly string[] resourceTerms = new string[] {
            "ambulance",
            "\"home quarantine\"",
            "hospital",
            "beds",
            "ventilator",
            "icu",
            //"remdesivir",
            "tocilizumab",
            "fabiflu",
            "favipiravir",
            "oxygen",
            "plasma",
            "tiffin"
        };
        private static readonly string[] locationTerms = new string[] {
            "allahabad",
            "prayagraj",
            "pune",
            "nashik",
            "nasik",
            "nagpur",
            "mumbai",
            "bombay",
            "chennai",
            "thane",
            "delhi",
            "gurgaon",
            "ghaziabad",
            "bengaluru",
            "bangalore",
            "lucknow",
            "varanasi",
            "kanpur",
            "indore",
            "ahmedabad",
            "amdavad",
            "patna",
            "bhopal",
            "hyderabad",
            "vijaywada",
            "kolkata"
        };
        private static readonly string[] exclusionTerms = new string[] {
            "need",
            "needed",
            "needs",
            "required",
            "require",
            "patient",
            "\"request for\"",
            "\"urgent help\"",
            "\"please help\"",
            "\"plz help\"",
            "\"pls help\"",
            "\"bot link\"",
            "requirement",
            "requirements",
            "relative",
            "symptoms",
            "\"help please\"",
            "\"can you help\"",
            "\"admitted to\"",
            "condition"
        };
        #endregion

        // separate exclusion terms here due to twitter length restriction on query string
        private static readonly string[] exclusionStrings = new string[] {
            "urgently looking",
            "needs a plasma donor",
            "any potential",
            "looking for a",
            "my close friend",
            "currently hospitalised",
            "help with an",
            "#urgenthelp"
        };
        private static readonly Dictionary<string, string[]> resourceAdditionalDetails = new() {
            ["plasma"] = new string[] { "[^a-z]a\\+", "[^a-z]a\\-", "[^a-z]b\\+", "[^a-z]b\\-", "[^a-z]o\\+", "[^a-z]o\\-", "[^a-z]ab\\+", "[^a-z]ab\\-" },
            ["oxygen"] = new string[] { "can", "cylinder", "concentrator", "refill" }
        };

        // twitter post phone number regex
        private static readonly string phoneNumberPattern = @"([\t -]*?[0-9][\t -]*?){10,}";

        // google sheets helper with spreadsheet id
        private static GoogleSheetsHelper googleSheet = new(Environment.GetEnvironmentVariable("GOOGLE_SHEET_ID"));

        // azure blob client
        private static BlobServiceClient blobServiceClient = new(Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING"));

        // list of historical unique phone numbers
        private static Dictionary<string, DateTimeOffset> historicalPhoneNumbers;
        private static Dictionary<string, DateTimeOffset> currentRunPhoneNumbers = new();

        // run this function once every two hours at the start of the hour, e.g. 12:00 am, 2:00 am, 4:00 am etc.
        // 0 0 */2 * * *
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

            logger.LogInformation($"Script started at: {DateTimeOffset.UtcNow}");
            logger.LogInformation($"Next timer schedule at: {myTimer.ScheduleStatus.Next}");

            // get the prior phone numbers pulled by the script
            await LoadUniquePhoneNumbersAsync();

            var usingPresetData = false;
            var twitterPosts = usingPresetData ? ReadLocalData() : await FetchTwitterDataAsync(TimeSpan.FromHours(2));

            LogData(twitterPosts);

            await SaveUniquePhoneNumbersAsync();

            AppendCompletionSpreadsheetRow();

            logger.LogInformation($"Script completed at {DateTimeOffset.UtcNow}");
        }

        public static async Task LoadUniquePhoneNumbersAsync() {
            var containerClient = blobServiceClient.GetBlobContainerClient("phone-numbers");
            var blobClient = containerClient.GetBlobClient("phoneNumbers.json");

            using (var destinationStream = new MemoryStream()) {
                await blobClient.DownloadToAsync(destinationStream);

                var destinationString = Encoding.UTF8.GetString(destinationStream.ToArray());

                historicalPhoneNumbers = JsonConvert.DeserializeObject<Dictionary<string, DateTimeOffset>>(destinationString);
            }
        }

        public static async Task SaveUniquePhoneNumbersAsync() {
            var containerClient = blobServiceClient.GetBlobContainerClient("phone-numbers");
            var blobClient = containerClient.GetBlobClient("phoneNumbers.json");

            var sourceString = JsonConvert.SerializeObject(historicalPhoneNumbers.Union(currentRunPhoneNumbers).ToDictionary(item => item.Key, item => item.Value));

            await blobClient.UploadAsync(new MemoryStream(Encoding.UTF8.GetBytes(sourceString)), overwrite: true);
        }

        public static async Task<List<TwitterPost>> FetchTwitterDataAsync(TimeSpan _postTimeRange) {
            var twitterPosts = new List<TwitterPost>();

            // find the tweets with the terms in phoneTerms + resourceTerms
            var twitterSearchResult = null as TwitterSearchResult;
            var startTime = DateTimeOffset.UtcNow.Subtract(_postTimeRange);
            var startTimeString = HttpUtility.UrlEncode(startTime.ToString("yyyy-MM-ddTHH:mm:ssZ"));
            var tweetFields = string.Join(",", new string[] { "id", "text", "created_at" });
            var iteration = 0;
            var maxResults = 100;
            var nextToken = string.Empty;

            for (var i = 0; i < locationTerms.Length; i++) {
                for (var j = 0; j < resourceTerms.Length; j++) {
                    var queryString = HttpUtility.UrlEncode(
                        "-is:retweet"
                     + $" {locationTerms[i]}"
                     + $" {resourceTerms[j]}"
                     + $" -{string.Join(" -", exclusionTerms)}"
                     + $" ({string.Join(" OR ", phoneTerms)})"
                    );

                    do {
                        var requestUri = $"https://api.twitter.com/2/tweets/search/recent?start_time={startTimeString}&query={queryString}&tweet.fields={tweetFields}&max_results={maxResults}";

                        requestUri += string.IsNullOrWhiteSpace(nextToken) ? string.Empty : $"&next_token={nextToken}";

                        using (var request = new HttpRequestMessage(HttpMethod.Get, requestUri)) {
                            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", Environment.GetEnvironmentVariable("TWITTER_API_OAUTH_TOKEN"));

                            using (var response = await httpClient.SendAsync(request)) {
                                if (response.StatusCode == System.Net.HttpStatusCode.TooManyRequests) {
                                    PauseTwitterApiQueries(iteration);

                                    // decrement i and j to retry the same request again after pausing above
                                    i--;
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
                                        logger.LogInformation($"no results returned when looking for posts with location {locationTerms[i]} and resource {resourceTerms[j]} after {startTime} UTC");
                                    }

                                    // sleep 200 ms to add delay to the requests
                                    Thread.Sleep(200);

                                    logger.LogInformation($"got {twitterSearchResult.meta.result_count} posts from twitter api on iteration {iteration}");

                                    iteration++;

                                    if ((iteration + 1) % 450 == 0) {
                                        PauseTwitterApiQueries(iteration);
                                    }
                                }
                            }
                        }

                        nextToken = twitterSearchResult?.meta?.next_token;
                    } while (twitterSearchResult.meta.next_token != null);
                }
            }

            logger.LogInformation($"got {twitterPosts.Count} total matching posts from twitter api after {iteration} iterations");

            return twitterPosts;
        }

        public static void PauseTwitterApiQueries(int iteration) {
            logger.LogInformation($"sleeping for 15 minutes, {iteration} requests have been made so far to twitter api");

            // if 450 requests have been made, sleep for 15 minutes until the next batch of requests can start
            // 1_000 ms (1 sec) * 60 sec/min * 15 min = 900 sec
            Thread.Sleep(1_000 * 60 * 15);

            logger.LogInformation($"finished sleeping for 15 minutes, resuming data pull");
        }

        public static List<TwitterPost> ReadLocalData() {
            var data = File.ReadAllText(@"C:\Users\patri\OneDrive\Desktop\CaregiverSaathiCovid19\cs-covid19-data-pull\repo\sampleData.json");

            return JsonConvert.DeserializeObject<List<TwitterPost>>(data);
        }

        public static void LogData(List<TwitterPost> _twitterPosts) {
            // record City/Resource/Text/Phone No./Date Tweeted
            var itemsLogged = 0;

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

                        var matchingLocations = locationTerms.Where(location => post.text.ToLower().Contains(location));
                        var matchingResources = resourceTerms.Where(resource => post.text.ToLower().Contains(resource));
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
                            }

                            // add row to the google spreadsheet
                            spreadsheetRows.Add(
                                new GoogleSheetRow() {
                                    Cells = new List<GoogleSheetCell>() {
                                        new GoogleSheetCell() { CellValue = string.Join(", ", matchingLocations.Select(location => CapitalizeWord(location))) },
                                        new GoogleSheetCell() { CellValue = string.Join(", ", matchingResources.Select(resource => CapitalizeWord(resource))) },
                                        new GoogleSheetCell() { CellValue = string.Join(", ", resourceDetails) },
                                        new GoogleSheetCell() { CellValue = number },
                                        new GoogleSheetCell() { CellValue = post.created_at },
                                        new GoogleSheetCell() { CellValue = post.text.Replace("\\n", "\n") },
                                    }
                                }
                            );

                            logger.LogInformation($"id: {post.id}, locations: {string.Join(", ", matchingLocations)}, resources: {string.Join(", ", matchingResources)}, phone #: {number}, date tweeted: {post.created_at}");
                            itemsLogged++;
                        } else {
                            logger.LogInformation($"id: {post.id}, duplicate phone number {number} found, not adding to spreadsheet");
                        }
                    }
                }

                googleSheet.AppendRows(spreadsheetRows, "Sheet1", logger);
            }

            logger.LogInformation($"logged {itemsLogged} combinations to output spreadsheet");
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
            return exclusionStrings.Any(term => _post.text.ToLower().Contains(term)) == false;
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

            googleSheet.AppendRows(new List<GoogleSheetRow>() { row }, "Sheet1", logger);
        }

        public static string CapitalizeWord(string word) {
            if (word.ToLower() == "icu") {
                return "ICU";
            } else if (word.ToLower() == "ab+") {
                return "AB+";
            } else if (word.ToLower() == "ab-") {
                return "AB-";
            } else {
                return word.Length >= 2 ? word.Substring(0, 1).ToUpper() + word[1..] : word.ToUpper();
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
