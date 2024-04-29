#r "Newtonsoft.Json"
#r "Microsoft.AnalysisServices.Tabular.dll"
#r "Microsoft.AnalysisServices.Core.dll"
#r "Azure.Identity.dll"
#r "Azure.Core.dll"

using Newtonsoft.Json;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AnalysisServices.Tabular;
using Azure.Identity;
using Azure.Core;

public static async Task<IActionResult> Run(HttpRequest req, ILogger log)
{
    log.LogInformation("Function started");

    // Declare parameters and assign values from the body
    string workspace = req.Query["workspace"];
    string dataset = req.Query["dataset"];
    string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
    dynamic data = JsonConvert.DeserializeObject(requestBody);
    workspace = workspace ?? data?.workspace;
    dataset = dataset ?? data?.dataset;

    // New parameter for table details
    List<dynamic> tables = data?.tables != null ? JsonConvert.DeserializeObject<List<dynamic>>(data.tables.ToString()) : null;

    log.LogInformation("Inputs loaded from the body");

    // Get AccessToken to authenticate via Managed Identity
    var tokenCredential = new DefaultAzureCredential();
    var resource = "https://analysis.windows.net/powerbi/api";
    var token = await tokenCredential.GetTokenAsync(new TokenRequestContext(new string[] { $"{resource}/.default" }));
    log.LogInformation("AccessToken fetched from https://analysis.windows.net/powerbi/api");

    // Build connection string + connect to the Power BI workspace
    string connStr = $"DataSource=powerbi://api.powerbi.com/v1.0/myorg/{workspace};Password={token.Token};";
    Server pbiserver = new Server();
    pbiserver.Connect(connStr);
    log.LogInformation("Connection to Power BI server established");

    try
    {
        // Iterate through tables and refresh either entire table or specific partitions
        Database db = pbiserver.Databases.GetByName(dataset);
        Model m = db.Model;

        // Use Parallel.ForEach to iterate through tables in parallel
        Parallel.ForEach(tables, tableDetails =>
        {
            string tableName = tableDetails?.name;
            bool refreshPartitions = tableDetails?.refreshPartitions ?? false;

            if (m.Tables.Contains(tableName))
            {
                if (refreshPartitions)
                {
                    // Refresh specific partitions
                    foreach (string partitionName in tableDetails.partitions)
                    {
                        if (m.Tables[tableName].Partitions.Contains(partitionName))
                        {
                            Partition partition = m.Tables[tableName].Partitions[partitionName];
                            partition.RequestRefresh(RefreshType.Full);
                        }
                        else
                        {
                            log.LogWarning($"Partition '{partitionName}' not found in the table '{tableName}'. Skipping.");
                        }
                    }
                }
                else
                {
                    // Refresh entire table
                    m.Tables[tableName].RequestRefresh(RefreshType.Full);
                }
            }
            else
            {
                log.LogWarning($"Table '{tableName}' not found in the dataset '{dataset}'. Skipping.");
            }
        });

        // Save changes and trigger refresh
        m.SaveChanges();
        log.LogInformation("Power BI dataset refresh triggered");
    }
    finally
    {
        // Disconnect from Power BI service in a finally block to ensure it happens even if an exception occurs
        pbiserver.Disconnect();
        log.LogInformation("Disconnected from Power BI service");
    }

    // Return response in JSON format
    string outputResponse =
        @"{
            ""workspace"": """ + workspace + @""",
            ""dataset"": """ + dataset + @""",
            ""tables"": " + JsonConvert.SerializeObject(tables) + @",
            ""response"": ""Refresh triggered for dataset " + dataset + " in workspace " + workspace + @"""
            }";
    return new OkObjectResult(outputResponse);
}
