// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// ------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.Samples.Bulk
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Configuration;

    // ----------------------------------------------------------------------------------------------------------
    // Prerequisites -
    //
    // 1. An Azure Cosmos account - 
    //    https://azure.microsoft.com/en-us/itemation/articles/itemdb-create-account/
    //
    // 2. Microsoft.Azure.Cosmos NuGet package - 
    //    http://www.nuget.org/packages/Microsoft.Azure.Cosmos/ 
    // ----------------------------------------------------------------------------------------------------------
    public class Program
    {
        private const string EndpointUrl = "https://mas-cosmos-db.documents.azure.com:443/";
        private const string AuthorizationKey = "gKjb1mRFp91EGP5zGzsUMnYeAMashA3B3YWCijgmACf7uiy4QefTxN36911qo5CIRj272bdadarBsGWPHDndWA==";
        private const string DatabaseName = "NoSQLEvaluationPOC-Scenario4";
        private static readonly string[] EntityTypeNames = { "items", "checklist_question" };
        private const int AmountToInsert = 300;
        private const int MaxRUPerSec = 4000;
        private const int DefaultRUPerSec = 4000;
        private enum ProgramMode {PrepareDatabase,LoadDatabase,ResetDatabaseDefaults,ProofOfConcept };
        private enum ProofOfConcept { HelloWorld, ReadAzureBlob, TestConfiguration};
        public class RepairReshaperFileOptions
        {
            /// <summary>
            /// Name of the blob container containing the template files.
            /// </summary>
            public string TemplateContainer { get; set; }
            /// <summary>
            /// Name of the blob container to write repaired files to.
            /// </summary>
            public string ResultsContainer { get; set; }
            /// <summary>
            /// Name of the blob container containing the Reshaper files to repair.
            /// </summary>
            public string InputContainer { get; set; }
            /// <summary>
            /// Number of rows to write to the repaired block blob.
            /// Increasing this number will reduce the number of writes to blob storage
            /// but will also increase the memory required for file processing.
            /// </summary>
            public int ResultBlockSize { get; set; }

            /// <summary>
            /// Blob storage connection string for reading and writing blobs.
            /// </summary>
            public string BlobStorageConnectionString { get; set; }
            /// <summary>
            /// Connection string for writing error logs
            /// </summary>
            public string ErrorLogDBConnectionString { get; set; }
        }
        static async Task Main(string[] args)
        {
            //Choose which mode to run (Prepare,Load,ResetDefaults)
            ProgramMode programMode = ProgramMode.ProofOfConcept;
            Console.WriteLine($"Program mode is {programMode.ToString()}.\n");

            IConfigurationRoot configuration = new ConfigurationBuilder()
            .SetBasePath("C:\\Users\\Mahesh.Sookram\\OneDrive - InEight\\Dev\\Repos\\cosmos-dotnet-bulk-import-throughput-optimizer\\src")
            .AddJsonFile("local.settings.json")
            .Build();


            RepairReshaperFileOptions settings = new RepairReshaperFileOptions();

            configuration.GetSection("LoadCompletionsDb").Bind(settings);
          

            //Obtain a client to the Cosmos DB account using the SDK
            CosmosClient cosmosClient = new CosmosClient(EndpointUrl, AuthorizationKey, new CosmosClientOptions() { AllowBulkExecution = true });

            //Either Prepare the database, load the database, or reset the database databaseContainers
            // to normal default throughputs depending on the program mode.
            switch (programMode)
            {
                case ProgramMode.PrepareDatabase:
                    await PrepareDatabaseAsync(cosmosClient,DatabaseName,EntityTypeNames,MaxRUPerSec);
                    break;
                case ProgramMode.LoadDatabase:
                    await LoadDatabaseAsync(cosmosClient, DatabaseName, EntityTypeNames);
                    break;
                case ProgramMode.ResetDatabaseDefaults:
                    await ResetDatabaseDefaultsAsync(cosmosClient, DatabaseName, EntityTypeNames, DefaultRUPerSec);
                    break;
                case ProgramMode.ProofOfConcept:
                    ProofOfConceptAsync(ProofOfConcept.TestConfiguration,settings);
                    break;
            }

            
        }

        private static void ProofOfConceptAsync(ProofOfConcept proofOfConcept, RepairReshaperFileOptions settings)
        {
            switch (proofOfConcept)
            {
                case ProofOfConcept.HelloWorld:
                    Console.WriteLine("Hello World!");
                    break;
                case ProofOfConcept.ReadAzureBlob:
                    break;
                case ProofOfConcept.TestConfiguration:
                    Console.WriteLine($"Setting InputContainer is: {settings.InputContainer}");
                    break;

            }
        }

        private static async Task PrepareDatabaseAsync(CosmosClient cosmosClient, string databaseName, string[] entityTypeNames, int maxRUPerSec)
        {
            Console.WriteLine("Preparing database for document loading...");
            //Create the database if it does not already exist
            Database database = await cosmosClient.CreateDatabaseIfNotExistsAsync(databaseName);

            //Prepare each database container to process for processing.
            Console.WriteLine($"There are {entityTypeNames.Count()} container(s) to process.");
            foreach (string entityTypeName in entityTypeNames)
            {
                await PrepareDatabaseContainerAsync(cosmosClient, database, entityTypeName, maxRUPerSec);
            }

        }

        private static async Task PrepareDatabaseContainerAsync(CosmosClient cosmosClient, Database database, string containerName, int maxRUPerSec)
        {
            //List the database databaseContainers
            List<String> databaseContainers = await ListContainerNamesInDatabaseAsync(database);
            bool containerExists = databaseContainers.Contains(containerName);
            Console.WriteLine($"Database \"{database.Id}\" {(containerExists ? "does" : "does not")} contain a container called \"{containerName}\".");
            Container container;
            if (!containerExists)
            {
                // Container and autoscale throughput settings
                ContainerProperties autoscaleContainerProperties = new ContainerProperties(containerName, "/partitionKey");
                ThroughputProperties autoscaleThroughputProperties = ThroughputProperties.CreateAutoscaleThroughput(maxRUPerSec); //Set autoscale max RU/s

                // Create the container with autoscale enabled
                container = await database.CreateContainerAsync(autoscaleContainerProperties, autoscaleThroughputProperties);
            }
            else
            {
                container = cosmosClient.GetContainer(DatabaseName, containerName);
            }

            //What is the current Thoughput
            int? currentRUPerSec = await container.ReadThroughputAsync();
            Console.WriteLine($"Container {containerName} throughput is currently {currentRUPerSec}.");

            //Set Container Max Throughput to MaxRUPerSec
            if (currentRUPerSec != MaxRUPerSec)
            {
                Console.WriteLine($"Setting container {containerName} throughput to {maxRUPerSec}.");
                await container.ReplaceThroughputAsync(ThroughputProperties.CreateAutoscaleThroughput(maxRUPerSec));
            }



        }
        private static async Task LoadDatabaseAsync(CosmosClient cosmosClient, string databaseName, string[] entityTypeNames)
        {
            Console.WriteLine("Loading Cosmos DB database with generated data...");
            //Create the database if it does not already exist
            Database database = await cosmosClient.CreateDatabaseIfNotExistsAsync(databaseName);

            //Prepare each database container to process for processing.
            Console.WriteLine($"There are {entityTypeNames.Count()} container(s) to process.");
            foreach (string entityTypeName in entityTypeNames)
            {
                await LoadDatabaseContainerAsync(cosmosClient, database, entityTypeName);
            }

        }

        private static async Task LoadDatabaseContainerAsync(CosmosClient cosmosClient, Database database, string containerName)
        {
            //List the database databaseContainers
            List<String> databaseContainers = await ListContainerNamesInDatabaseAsync(database);
            bool containerExists = databaseContainers.Contains(containerName);
            Console.WriteLine($"Database \"{database.Id}\" {(containerExists ? "does" : "does not")} contain a container called \"{containerName}\".");

            if (containerExists)
            {
                Container container = cosmosClient.GetContainer(DatabaseName, containerName);
                //What is the current Thoughput?
                int? currentRUPerSec = await container.ReadThroughputAsync();
                Console.WriteLine($"Container {containerName} throughput is currently {currentRUPerSec}.");
                try
                {
                    // Prepare items for insertion
                    Console.WriteLine($"Preparing {AmountToInsert} items to insert...");
                    // <Operations>
                    var itemsToInsert = Program.GetItemsToInsert(containerName);
                    // </Operations>

                    // Create the list of Tasks
                    Console.WriteLine($"Starting...");
                    Stopwatch stopwatch = Stopwatch.StartNew();
                    // <ConcurrentTasks>
                    List<Task> tasks = new List<Task>(AmountToInsert);
                    
                    foreach (var item in itemsToInsert)
                    {
                        tasks.Add(container.CreateItemAsync(item, new PartitionKey(((IHasPartitionKey) item).partitionKey))
                            .ContinueWith(itemResponse =>
                            {
                                if (!itemResponse.IsCompletedSuccessfully)
                                {
                                    AggregateException innerExceptions = itemResponse.Exception.Flatten();
                                    if (innerExceptions.InnerExceptions.FirstOrDefault(innerEx => innerEx is CosmosException) is CosmosException cosmosException)
                                    {
                                        Console.WriteLine($"Received {cosmosException.StatusCode} ({cosmosException.Message}).");
                                    }
                                    else
                                    {
                                        Console.WriteLine($"Exception {innerExceptions.InnerExceptions.FirstOrDefault()}.");
                                    }
                                }
                            }));
                    }
                    // Wait until all are done
                    await Task.WhenAll(tasks);
                    // </ConcurrentTasks>
                    stopwatch.Stop();

                    Console.WriteLine($"Finished in writing {AmountToInsert} items in {stopwatch.Elapsed}.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                }


            }

        }

        private static async Task ResetDatabaseDefaultsAsync(CosmosClient cosmosClient, string databaseName, string[] containerNames, int defaultRUPerSec)
        {
            Console.WriteLine("Resetting Cosmos DB database containers back to default throughput levels...");
            //Create the database if it does not already exist
            Database database = await cosmosClient.CreateDatabaseIfNotExistsAsync(databaseName);

            //Prepare each database container to process for processing.
            Console.WriteLine($"There are {containerNames.Count()} container(s) to process.");
            foreach (string containerName in containerNames)
            {
                await ResetDatabaseContainerDefaultsAsync(cosmosClient, database, containerName, defaultRUPerSec);
            }


        }

        private static async Task ResetDatabaseContainerDefaultsAsync(CosmosClient cosmosClient, Database database, string containerName, int defaultRUPerSec)
        {
            //List the database databaseContainers
            List<String> databaseContainers = await ListContainerNamesInDatabaseAsync(database);
            bool containerExists = databaseContainers.Contains(containerName);
            Console.WriteLine($"Database \"{database.Id}\" {(containerExists ? "does" : "does not")} contain a container called \"{containerName}\".");

            if (containerExists)
            {
                Container container = cosmosClient.GetContainer(DatabaseName, containerName);

                //What is the current Thoughput
                int? currentRUPerSec = await container.ReadThroughputAsync();
                Console.WriteLine($"Container {containerName} throughput is currently {currentRUPerSec}.");

                //Set Container Max Throughput to MaxRUPerSec
                if (currentRUPerSec != defaultRUPerSec)
                {
                    Console.WriteLine($"Setting container {containerName} throughput to {defaultRUPerSec}.");
                    await container.ReplaceThroughputAsync(ThroughputProperties.CreateAutoscaleThroughput(defaultRUPerSec));
                }

            }


        }



        private static async Task<List<String>> ListContainerNamesInDatabaseAsync(Database database)
        {
            List<String> result = new List<String>();
            using (FeedIterator<ContainerProperties> resultSetIterator = database.GetContainerQueryIterator<ContainerProperties>())
            {
                while (resultSetIterator.HasMoreResults)
                {
                    foreach (ContainerProperties container in await resultSetIterator.ReadNextAsync())
                    {
                        result.Add(container.Id);

                    }
                }
            }

            return result;
        }
        private static IReadOnlyCollection<object> GetItemsToInsert(string entityTypeName)
        {
            switch (entityTypeName)
            {
                
                case "items":
                    return new Bogus.Faker<Item>()
                        .StrictMode(true)
                        //Generate item
                        .RuleFor(o => o.id, f => Guid.NewGuid().ToString()) //id
                        .RuleFor(o => o.username, f => f.Internet.UserName())
                        .RuleFor(o => o.partitionKey, (f, o) => "Partition 1") //partitionKey
                        .Generate(AmountToInsert);
                    
                case "checklist_question":
                    Random r = new Random();
                    return new Bogus.Faker<ChecklistQuestion>()
                        .StrictMode(true)
                        //Generate item
                        .RuleFor(o => o.instance_id, f => Guid.NewGuid().ToString())
                        .RuleFor(o=>o.created_date, f=> DateTimeOffset.Now.ToUnixTimeSeconds())
                        .RuleFor(o => o.created_by, f => f.Internet.UserName())
                        .RuleFor(o => o.last_modified_date, f => DateTimeOffset.Now.ToUnixTimeSeconds())
                        .RuleFor(o => o.last_modified_by, f => f.Internet.UserName())
                        .RuleFor(o => o.template_name, f => $"Template name {r.Next(0, 10)}")
                        .RuleFor(o => o.parent_instance_id, f => Guid.NewGuid().ToString())
                        .RuleFor(o => o.question_text, f => $"Question Text {r.Next(0, 10)}")
                        .RuleFor(o => o.question_type, f => $"Question Type {r.Next(0, 10)}")
                        .RuleFor(o => o.input_range, f => $"Input Range {r.Next(0, 10)}")
                        .RuleFor(o => o.resolution_criteria, f => $"Resolution Criteria {r.Next(0, 10)}")
                        .RuleFor(o => o.category_id, f => Guid.NewGuid().ToString())
                        .RuleFor(o => o.sort_order, f => r.Next(0, 10))
                        .RuleFor(o => o.firm, f => $"Firm {r.Next(0, 100)}")
                        .RuleFor(o => o.firm_id, f => Guid.NewGuid().ToString())
                        .RuleFor(o => o.project, f => $"Project {r.Next(0, 100)}")
                        .RuleFor(o => o.project_id, f => Guid.NewGuid().ToString())
                        .RuleFor(o => o.table_version, f => Guid.NewGuid().ToString())
                        .RuleFor(o => o.last_sync_date, f => DateTimeOffset.Now.ToUnixTimeSeconds())
                        .RuleFor(o => o.id, (f,o) => o.instance_id) //id
                        .RuleFor(o => o.partitionKey, (f, o) => $"{o.project_id}:{o.firm_id}") //partitionKey
                        .Generate(AmountToInsert);
                default:
                    Console.WriteLine($"Container {entityTypeName} has no data generator.");
                    return new List<Item>();

            }
            
        }
        public interface IHasPartitionKey
        {
            public string partitionKey { get; set; }
        }
        
        public class Item : IHasPartitionKey
        {
            public string id { get; set; }
            public string partitionKey { get; set; }
            public string username { get; set; }
        }

        public class ChecklistQuestion : IHasPartitionKey
        {
            public string instance_id { get; set; }
            public long created_date { get; set; }
            public string created_by { get; set; }
            public long last_modified_date { get; set; }
            public string last_modified_by { get; set; }
            public string template_name { get; set; }
            public string parent_instance_id { get; set; }
            public string question_text { get; set; }
            public string question_type { get; set; }
            public string input_range { get; set; }
            public string resolution_criteria { get; set; }
            public string category_id { get; set; }
            public int sort_order { get; set; }
            public string firm { get; set; }
            public string firm_id { get; set; }
            public string project { get; set; }
            public string project_id { get; set; }
            public string table_version { get; set; }
            public long last_sync_date { get; set; }
            public string id { get; set; }
            public string partitionKey { get; set; }
        }

        public class ChecklistQuestionResponse : IHasPartitionKey
        {
            public string instance_id { get; set; }
            public long created_date { get; set; }
            public string created_by { get; set; }
            public long last_modified_date { get; set; }
            public string last_modified_by { get; set; }
            public string parent_instance_id { get; set; }
            public string template_id { get; set; }
            public string signee_user_name { get; set; }
            public string signee_team_name { get; set; }
            public string string_answer { get; set; }
            public string guid_answer { get; set; }
            public long date_time_answer { get; set; }
            public decimal float_answer { get; set; }
            public string firm { get; set; }
            public string firm_id { get; set; }
            public string project { get; set; }
            public string project_id { get; set; }
            public string table_version { get; set; }
            public long last_sync_date { get; set; }
            public string id { get; set; }
            public string partitionKey { get; set; }
        }


    }


}



