using System;
using System.Globalization;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using CsvHelper;
using CsvHelper.Configuration;

class Program
{
    // ----- Configuration -----
    // Prefer ENV VARS; falls back to hardcoded defaults if you set them here.
    // Example: setx WEBEX_BASE https://webexapis.com
    //          setx WEBEX_TOKEN <YOUR_ADMIN_TOKEN>
    //          setx WEBEX_ORG_ID <YOUR_ORG_ID>
    private static readonly string BASE =
        Environment.GetEnvironmentVariable("WEBEX_BASE") ?? "https://webexapis.com";

    // If you insist on hard-coding, put the token/orgId here — but ENV VARS are safer.
    
    private static readonly string TOKEN =
        Environment.GetEnvironmentVariable("WEBEX_TOKEN") ?? ""; // "<YOUR TOKEN>";

    private static readonly string ORG_ID =
        Environment.GetEnvironmentVariable("WEBEX_ORG_ID") ?? ""; // "<YOUR ORG_ID>";
    

    private static readonly HttpClient http = new HttpClient
    {
        Timeout = TimeSpan.FromSeconds(30)
    };

    static async Task<int> Main(string[] args)
    {
        // ----- Inputs/Outputs -----
        string inputCsv = "email2personID.csv";
        string outputCsv = "userVoicemailParameters.csv";

        if (string.IsNullOrWhiteSpace(TOKEN))
        {
            Console.Error.WriteLine("ERROR: Set the WEBEX_TOKEN environment variable to a valid admin token.");
            return 1;
        }

        if (!File.Exists(inputCsv))
        {
            Console.Error.WriteLine($"CSV file not found: {inputCsv}");
            return 1;
        }

        // ----- HTTP headers -----
        http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", TOKEN);
        http.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

        // ----- CSV setup -----
        var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = true,
            TrimOptions = TrimOptions.Trim,
            BadDataFound = null
        };

        // Prepare writer with headers that mirror your Python fields
        using var writer = new StreamWriter(outputCsv);
        using var csvOut = new CsvWriter(writer, csvConfig);
        csvOut.WriteField("email");
        csvOut.WriteField("personid");
        csvOut.WriteField("enabled");
        csvOut.WriteField("storageType");
        csvOut.WriteField("mwiEnabled");
        csvOut.WriteField("externalEmail");
        csvOut.WriteField("emailCopyEnabled");
        csvOut.WriteField("emailCopyId");
        await csvOut.NextRecordAsync();

        // Read input and process rows
        using var reader = new StreamReader(inputCsv);
        using var csvIn = new CsvReader(reader, csvConfig);
        await foreach (var row in csvIn.GetRecordsAsync<dynamic>())
        {
            // dynamic row behaves like IDictionary<string, object>
            var dict = (System.Collections.Generic.IDictionary<string, object>)row;

            string email = (dict.TryGetValue("email", out var e) ? Convert.ToString(e) : "")?.Trim() ?? "";
            string personId = (dict.TryGetValue("personid", out var p) ? Convert.ToString(p) : "")?.Trim() ?? "";

            if (string.IsNullOrWhiteSpace(personId))
            {
                Console.WriteLine($"⚠️  Skipping missing personid for {email}");
                continue;
            }

            var data = await GetVoicemailForPersonAsync(personId);
            if (data is null) continue;

            // Extract the same fields as the Python script
            bool? enabled = data["enabled"]?.GetValue<bool?>();

            var messageStorage = data["messageStorage"] as JsonObject;
            string? storageType = messageStorage?["storageType"]?.GetValue<string?>();
            bool? mwiEnabled = messageStorage?["mwiEnabled"]?.GetValue<bool?>();
            string? externalEmail = messageStorage?["externalEmail"]?.GetValue<string?>();

            var emailCopy = data["emailCopyOfMessage"] as JsonObject;
            bool? emailCopyEnabled = emailCopy?["enabled"]?.GetValue<bool?>();
            string? emailCopyId = emailCopy?["emailId"]?.GetValue<string?>();

            // Write CSV row
            csvOut.WriteField(email);
            csvOut.WriteField(personId);
            csvOut.WriteField(NullableToString(enabled));
            csvOut.WriteField(storageType ?? "");
            csvOut.WriteField(NullableToString(mwiEnabled));
            csvOut.WriteField(externalEmail ?? "");
            csvOut.WriteField(NullableToString(emailCopyEnabled));
            csvOut.WriteField(emailCopyId ?? "");
            await csvOut.NextRecordAsync();

            // Console output
            Console.WriteLine($"\n📧 {email} ({personId})");
            Console.WriteLine($"  Voicemail Enabled: {NullableToString(enabled)}");
            Console.WriteLine($"  Storage Type: {storageType}");
            Console.WriteLine($"  MWI Enabled: {NullableToString(mwiEnabled)}");
            Console.WriteLine($"  External Email: {externalEmail}");
            Console.WriteLine($"  Email Copy Enabled: {NullableToString(emailCopyEnabled)}");
            Console.WriteLine($"  Email Copy ID: {emailCopyId}");
        }

        Console.WriteLine($"\n✅ All results written to {outputCsv}");
        return 0;
    }

    private static async Task<JsonObject?> GetVoicemailForPersonAsync(string personId)
    {
        var url = $"{BASE}/v1/people/{Uri.EscapeDataString(personId)}/features/voicemail";
        if (!string.IsNullOrWhiteSpace(ORG_ID))
        {
            url += $"?orgId={Uri.EscapeDataString(ORG_ID)}";
        }

        try
        {
            using var resp = await http.GetAsync(url);
            var payload = await resp.Content.ReadAsStringAsync();

            if (resp.IsSuccessStatusCode)
            {
                var node = JsonNode.Parse(payload) as JsonObject;
                return node;
            }
            else
            {
                Console.WriteLine($"❌ {personId}: {(int)resp.StatusCode} {payload}");
                return null;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ {personId}: Exception {ex.Message}");
            return null;
        }
    }

    private static string NullableToString(bool? b) => b.HasValue ? b.Value.ToString() : "";
}
