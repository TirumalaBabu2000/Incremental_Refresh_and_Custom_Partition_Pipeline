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

    // declare parameters and assign value from body
    string connectionstring = req.Query["connectionstring"];
    string datasource = req.Query["datasource"];
    string workspace = req.Query["workspace"];
    string dataset = req.Query["dataset"];
    string table = req.Query["table"];
    string partition = req.Query["partition"];
    string sourceobject = req.Query["sourceobject"];
    string partitionstatement = req.Query["partitionstatement"];

    string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
    dynamic data = JsonConvert.DeserializeObject(requestBody);

    connectionstring = connectionstring ?? data?.connectionstring;
    datasource = datasource ?? data?.datasource;
    workspace = workspace ?? data?.workspace;
    dataset = dataset ?? data?.dataset;
    table = table ?? data?.table;
    partition = partition ?? data?.partition;
    sourceobject = sourceobject ?? data?.sourceobject;
    partitionstatement = partitionstatement ?? data?.partitionstatement;
    log.LogInformation("input from body loaded into parameters");

    // get AccessToken to authenticate via Managed Identity
    var tokenCredential = new DefaultAzureCredential();
    var resource = "https://analysis.windows.net/powerbi/api";
    var token = await tokenCredential.GetTokenAsync( new TokenRequestContext( new string[] { $"{resource}/.default" }));
    log.LogInformation("AccessToken fetched from https://analysis.windows.net/powerbi/api");

    // build connection string + connect to the Power BI workspace
    string connStr = $"DataSource=powerbi://api.powerbi.com/v1.0/myorg/{workspace};Password={token.Token};";
    Server pbiserver = new Server();
    pbiserver.Connect(connStr);
    log.LogInformation("Connection to Power BI server established");

    // define variables
    Database db = pbiserver.Databases.GetByName(dataset);
    Model m = db.Model;
    Table t = m.Tables[table];
    DateTime currentDate = DateTime.Now;
    int currentYear = currentDate.Year;
    int currentMonth = currentDate.Month;
    int previousyear = currentYear-1;
    log.LogInformation("Power BI objects saved in variables");

    // add data source if it does not exist yet
    if (!(m.DataSources.Contains(datasource)))
    {   
        m.DataSources.Add(new ProviderDataSource()
        {
            Name = datasource,
            ConnectionString = connectionstring,
            ImpersonationMode = ImpersonationMode.ImpersonateServiceAccount,
            Provider = "System.Data.SQLClient"

        });
        log.LogInformation("Data source added");
    };
    if (!(t.Partitions.Contains("3P")))
    	{
			t.Partitions.Add
            	(   
                	new Partition()
                	{
                    	Name = "3P",
	                    Source = new QueryPartitionSource()
    	                {
        	                DataSource = m.DataSources[datasource],
            	            Query = @"SELECT * FROM " + sourceobject + " WHERE Is_Imported='Y'",
                	    }
	                }
    		    );
        
		};
    //creating yearly partition if not exist in the Table till year before previous year
    //DateTime currentDate = new DateTime(2022, 04, 25);
    for (int year=2020;year<previousyear;year++)
    {   
        if (!(t.Partitions.Contains(year.ToString())))
        {
            t.Partitions.Add
                (   
                    new Partition()
                    {
                        Name = year.ToString(),
                        Source = new QueryPartitionSource()
                        {
                            DataSource = m.DataSources[datasource],
                            Query = @"SELECT * FROM " + sourceobject +" WHERE Is_Imported='N' and YEAR(INV_DATE) ="+year,
                        }
                	}
        		);
        
		};
    }
	log.LogInformation("Partition added");
	
	// deleting Previous year month partition and creating a single year partition after currentYear March month.
	if (currentMonth>3)
	{
		for (int i=1;i<=9;i++)
		{
			if (t.Partitions.Contains(previousyear+"0"+i.ToString()))  
    		{   
    	    	t.Partitions.Remove(previousyear+"0"+i.ToString());
		        log.LogInformation("Partition dropped");
			//
    		};
		}
		for (int i=10;i<=12;i++)
		{
			if (t.Partitions.Contains(previousyear+i.ToString()))  
    		{   
    	    	t.Partitions.Remove(previousyear+i.ToString());
		        log.LogInformation("Partition dropped");
			
    		};
		}
		if (!(t.Partitions.Contains(previousyear.ToString())))
    	{
			t.Partitions.Add
            	(   
                	new Partition()
                	{
                    	Name = previousyear.ToString(),
                    	Source = new QueryPartitionSource()
                    	{
                        	DataSource = m.DataSources[datasource],
                        	Query = @"SELECT * FROM " + sourceobject +" WHERE Is_Imported='N' and YEAR(INV_DATE) ="+previousyear,
                    	}
                	}
        		);
        
		};
	};
	
	//creating Partitions for current and next year
	int nextyear=currentYear+1;
	for (int i=4;i<=9;i++)
    {   
		if (!(t.Partitions.Contains(currentYear+"0"+i.ToString())))
    	{
			t.Partitions.Add
            	(   
                	new Partition()
                	{
                    	Name = currentYear+"0"+i.ToString(),
                    	Source = new QueryPartitionSource()
                    	{
                        	DataSource = m.DataSources[datasource],
                        	Query = @"SELECT * FROM " + sourceobject +" WHERE Is_Imported='N' and YEAR(INV_DATE) ="+currentYear +" and Month(INV_DATE)="+i,
                    	}
                	}
        		);
        
		};
    }
	for (int i=10;i<=12;i++)
    {   
		if (!(t.Partitions.Contains(currentYear+i.ToString())))
    	{
			t.Partitions.Add
            	(   
                	new Partition()
                	{
                    	Name = currentYear+i.ToString(),
                    	Source = new QueryPartitionSource()
                    	{
                        	DataSource = m.DataSources[datasource],
                        	Query = @"SELECT * FROM " + sourceobject +" WHERE Is_Imported='N' and YEAR(INV_DATE) ="+currentYear +" and Month(INV_DATE)="+i,
                    	}
                	}
        		);
        
		};
    }
    for (int i=1;i<=4;i++)
    {   
		if (!(t.Partitions.Contains(nextyear+"0"+i.ToString())))
    	{
			t.Partitions.Add
            	(   
                	new Partition()
                	{
                    	Name = nextyear+"0"+i.ToString(),
                    	Source = new QueryPartitionSource()
                    	{
                        	DataSource = m.DataSources[datasource],
                        	Query = @"SELECT * FROM " + sourceobject +" WHERE Is_Imported='N' and YEAR(INV_DATE) ="+nextyear +" and Month(INV_DATE)="+i,
                    	}
                	}
        		);
        
		};
    }
	
	// save changes to Power BI service
    db.Model.SaveChanges();
    log.LogInformation("Changes saved to Power Bi service");

    // disconnect from Power BI service
    pbiserver.Disconnect(); 
    log.LogInformation("Disconnected from Power BI service ");

    // return response in JSON format
    string outputResponse = 
        @"{ 
            ""workspace"": """ + workspace + @""",
            ""dataset"": """ + dataset + @""",
            ""table"": """ + table + @""",
            ""partition"": """ + partition + @""",
            ""response"": ""Succesfully updated partition definition to " + partitionstatement + @"""
            }";
    return new OkObjectResult(outputResponse);
}