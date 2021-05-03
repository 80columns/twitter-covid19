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
            "remdesivir",
            "tocilizumab",
            "fabiflu",
            "favipiravir",
            "oxygen",
            "plasma",
            "tiffin"
        };
        private static readonly string[] locationTerms = new string[] {
            "mumbai",
            "delhi",
            "pune",
            "nagpur",
            "nashik",
            "bengaluru",
            "bangalore",
            "hyderabad",
            "faridagad",
            "gurgaon",
            "kanpur",
            "varanasi",
            "prayagraj",
            "agra",
            "jaipur"
        };
        #endregion

        private static readonly string[] exclusionStrings = new string[] {
            "need",
            "needed",
            "required",
            "urgent help",
            "please help",
            "patient",
            "plz help",
            "pls help",
            "admitted to",
            "immediate requirement",
            "bot link",
            "urgently looking",
            "needs a plasma donor",
            "urgent requirement",
            "urgently looking",
            "any potential",
            "looking for a",
            "my close friend",
            "condition is severe",
            "currently hospitalised",
            "needs icu"
        };
        private static readonly Dictionary<string, string[]> resourceAdditionalDetails = new() {
            ["plasma"] = new string[] { "[^a-z]a\\+", "[^a-z]a\\-", "[^a-z]b\\+", "[^a-z]b\\-", "[^a-z]o\\+", "[^a-z]o\\-", "[^a-z]ab\\+", "[^a-z]ab\\-" },
            ["oxygen"] = new string[] { "can", "cylinder", "concentrator", "refill" }
        };

        // twitter post phone number regex
        private static readonly string phoneNumberPattern = @"([\t -]*?[0-9][\t -]*?){10,}";

        // google sheets helper with spreadsheet id
        private static GoogleSheetsHelper googleSheet = new(Environment.GetEnvironmentVariable("GOOGLE_SHEET_ID"));

        // run this function once every two hours at the start of the hour, e.g. 12:00 am, 2:00 am, 4:00 am etc.
        // 0 0 */2 * * *
        // see https://docs.microsoft.com/en-us/azure/azure-functions/functions-bindings-timer?tabs=csharp for explanation of cron format
        // for testing, can use this to run every minute
        // 0 */1 * * * *

        // TODO:
        // - check phone numbers already in spreadsheet and don't create new entries for those phone numbers in the future
        [Function("TwitterDataPull")]
        public static async Task Run(
            [TimerTrigger("0 0 */2 * * *"
                #if DEBUG
                , RunOnStartup=true // https://stackoverflow.com/a/51775445
                #endif
            )] MyInfo myTimer, FunctionContext context) {

            #if DEBUG
                // force-launch visual studio debugger in debug mode, otherwise breakpoints will not be caught (issue with .NET 5 debugging in az functions)
                Debugger.Launch();
            #endif

            logger = context.GetLogger("TwitterDataPull");

            logger.LogInformation($"C# Timer trigger function executed at: {DateTimeOffset.UtcNow}");
            logger.LogInformation($"Next timer schedule at: {myTimer.ScheduleStatus.Next}");

            var usingPresetData = false;
            var twitterPosts = usingPresetData ? ReadLocalData() : await FetchTwitterData(TimeSpan.FromMinutes(30));

            LogData(twitterPosts);
        }

        public static async Task<List<TwitterPost>> FetchTwitterData(TimeSpan postTimeRange) {
            var twitterPosts = new List<TwitterPost>();

            // find the tweets with the terms in phoneTerms + resourceTerms
            var twitterSearchResult = null as TwitterSearchResult;
            var startTime = DateTimeOffset.UtcNow.Subtract(postTimeRange);
            var startTimeString = HttpUtility.UrlEncode(startTime.ToString("yyyy-MM-ddTHH:mm:ssZ"));
            var tweetFields = string.Join(",", new string[] { "id", "text", "created_at" });
            var iteration = 0;
            var maxResults = 100;

            foreach (var location in locationTerms) {
                var queryString = HttpUtility.UrlEncode(
                    "-is:retweet"
                    + $" {location}"
                    + $" ({string.Join(" OR ", phoneTerms)})"
                    + $" ({string.Join(" OR ", resourceTerms)})"
                );

                do {
                    using (var request = new HttpRequestMessage(HttpMethod.Get, $"https://api.twitter.com/2/tweets/search/recent?start_time={startTimeString}&query={queryString}&tweet.fields={tweetFields}&max_results={maxResults}")) {
                        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", Environment.GetEnvironmentVariable("TWITTER_API_OAUTH_TOKEN"));

                        using (var response = await httpClient.SendAsync(request)) {
                            var responseString = await response.Content.ReadAsStringAsync();
                            twitterSearchResult = JsonConvert.DeserializeObject<TwitterSearchResult>(responseString);

                            if (
                                twitterSearchResult?.data != null
                                && twitterSearchResult.data.Any()
                            ) {
                                twitterPosts.AddRange(twitterSearchResult.data.Where(post => CheckPhoneNumber(post)).Where(post => CheckExclusionTerms(post));
                            } else {
                                logger.LogInformation($"no results returned when looking for posts with location {location} after {startTime} UTC");
                            }
                        }
                    }

                    // sleep 200 ms to add delay to the requests
                    Thread.Sleep(200);

                    logger.LogInformation($"got {twitterSearchResult.meta.result_count} posts from twitter api on iteration {iteration}");

                    iteration++;

                    if ((iteration + 1) % 450 == 0) {
                        logger.LogInformation($"sleeping for 15 minutes, {iteration} requests have been made so far");

                        // if 450 requests have been made, sleep for 15 minutes until the next batch of requests can start
                        // 1_000 ms (1 sec) * 60 sec/min * 15 min = 900 sec
                        Thread.Sleep(1_000 * 60 * 15);

                        logger.LogInformation($"finished sleeping for 15 minutes, resuming data pull");
                    }
                } while (twitterSearchResult.meta.next_token != null);
            }

            logger.LogInformation($"got {twitterPosts.Count} total matching posts from twitter api after {iteration} iterations");

            return twitterPosts;
        }

        public static List<TwitterPost> ReadLocalData() {
            var data = File.ReadAllText(@"C:\Users\patri\OneDrive\Desktop\CaregiverSaathiCovid19\cs-covid19-data-pull\twitter\sampleData.json");

            return JsonConvert.DeserializeObject<List<TwitterPost>>(data);
        }

        public static void LogData(List<TwitterPost> _twitterPosts) {
            // record City/Resource/Text/Phone No./Date Tweeted
            var itemsLogged = 0;

            foreach (var post in _twitterPosts) {
                var phoneNumberMatches = Regex.Matches(post.text, phoneNumberPattern);

                foreach (Match numberMatch in phoneNumberMatches) {
                    var number = numberMatch.Value.Trim().Replace(" ", string.Empty).Replace("-", string.Empty);

                    if (number.Length == 11 && number[0] == '0') {
                        // handle cell phone numbers starting with 0
                        number = number.Substring(1, number.Length - 1);
                    } else if (number.Length == 12 && number.Substring(0, 2) == "91") {
                        // handle numbers starting with the country code +91
                        number = number.Substring(2, number.Length - 2);
                    }

                    var matchingLocations = locationTerms.Where(location => post.text.ToLower().Contains(location));
                    var matchingResources = resourceTerms.Where(resource => post.text.ToLower().Contains(resource));
                    var additionalDetails = new List<string>();

                    foreach (var resource in matchingResources) {
                        if (resourceAdditionalDetails.ContainsKey(resource)) {
                            foreach (var additionalItem in resourceAdditionalDetails[resource]) {
                                if (Regex.IsMatch(post.text.ToLower(), additionalItem)) {
                                    if (resource == "plasma") {
                                        additionalDetails.Add(CapitalizeWord(additionalItem[(additionalItem.IndexOf(']') + 1)..].Replace("\\", string.Empty)));
                                    } else {
                                        additionalDetails.Add(CapitalizeWord(additionalItem));
                                    }
                                }
                            }
                        }
                    }

                    AddSpreadsheetRow(
                        string.Join(", ", matchingLocations.Select(location => CapitalizeWord(location))),
                        string.Join(", ", matchingResources.Select(resource => CapitalizeWord(resource))),
                        number,
                        post.created_at,
                        post.text,
                        string.Join(", ", additionalDetails)
                    );

                    logger.LogInformation($"id: {post.id}, locations: {string.Join(", ", matchingLocations)}, resources: {string.Join(", ", matchingResources)}, phone #: {number}, date tweeted: {post.created_at}");
                    itemsLogged++;
                }
            }

            logger.LogInformation($"logged {itemsLogged} combinations to output spreadsheet");
        }

        public static bool CheckPhoneNumber(TwitterPost _post) {
            logger.LogInformation($"checking tweet {_post.id} for match");

            var phoneNumberMatch = Regex.IsMatch(_post.text, @"([\s-]*?[0-9][\s-]*?){10,}");

            if (phoneNumberMatch) {
                logger.LogInformation($"tweet {_post.id} matched");
            }

            return phoneNumberMatch;
        }

        public static bool CheckExclusionTerms(TwitterPost _post) {
            return exclusionStrings.Any(term => _post.text.ToLower().Contains(term)) == false;
        }

        public static void AddSpreadsheetRow(
            string _locations,
            string _resources,
            string _phoneNumber,
            string _dateTweeted,
            string _tweetText,
            string _additionalDetails
        ) {
            var row = new GoogleSheetRow() {
                Cells = new List<GoogleSheetCell>() {
                    new GoogleSheetCell() { CellValue = _locations },
                    new GoogleSheetCell() { CellValue = _resources },
                    new GoogleSheetCell() { CellValue = _phoneNumber },
                    new GoogleSheetCell() { CellValue = _dateTweeted },
                    new GoogleSheetCell() { CellValue = _tweetText.Replace("\\n", "\n") },
                    new GoogleSheetCell() { CellValue = _additionalDetails }
                }
            };

            googleSheet.AppendRows(new List<GoogleSheetRow>() { row }, "Sheet1");
        }

        public static string CapitalizeWord(string word) {
            if (word.ToLower() == "icu") {
                return "ICU";
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
