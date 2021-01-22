using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Azure.Storage.Blobs;

namespace FunctionApp7
{
    public static class Function1
    {
        const string EventHubsConnectionString = "Endpoint=sb://sample-eventhub-namespace-xuhliu.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=UK2TC+aA2ZGXkyfMoNXLzq5qDVJrDxiZdf1OCrNOyJg=;EntityPath=sample-eventhub";
        const string EventHubName = "sample-eventhub";
        const string ConsumerGroupName = "$Default";
        const string blobStorageConnectionString = "DefaultEndpointsProtocol=https;AccountName=cmob;AccountKey=5/Yh+YVzXOH/+FcdXpjCP/ND4hUyghJxSqrSvq18+tH2A9uTySnLZW4jQD9b1zsBx4/IJhbGMvaP4TTsupNJ+Q==;EndpointSuffix=core.windows.net";
        const string blobContainerName = "cmob";



        [FunctionName("Function1")]
        public static void Run([TimerTrigger("*/45 * * * * *")] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"every 30 second C# Timer trigger function executed at: {DateTime.Now}");

            // You better dissover partitions by eventhub client. I am just hardcoding them here for now.
            var partitions = new List<string> { "0", "1" };
            var receiveTasks = new List<Task>();

            foreach (var p in partitions)
            {
                receiveTasks.Add(ReadEventsFromPartition(p));
            }

            // Wait until all reads complete.
            Task.WhenAll(receiveTasks);
        }

        public static async Task ReadEventsFromPartition(string partitionId)
        {

            // Read from the default consumer group: $Default
            string consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

            // Create a blob container client that the event processor will use 
            BlobContainerClient storageClient = new BlobContainerClient(blobStorageConnectionString, blobContainerName);

            // Create an event processor client to process events in the event hub
            EventProcessorClient processor = new EventProcessorClient(storageClient, ConsumerGroupName, EventHubsConnectionString, EventHubName);


            // Register handlers for processing events and handling errors
            processor.ProcessEventAsync += ProcessEventHandler;
            processor.ProcessErrorAsync += ProcessErrorHandler;

            // Start the processing
            await processor.StartProcessingAsync();

            // Wait for 10 seconds for the events to be processed
            await Task.Delay(TimeSpan.FromSeconds(10));

            // Stop the processing
            await processor.StopProcessingAsync();

        }

        static async Task ProcessEventHandler(ProcessEventArgs eventArgs)
        {
            // Write the body of the event to the console window
            Console.WriteLine("\tReceived event: {0}", System.Text.Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray()));

            // Update checkpoint in the blob storage so that the app receives only new events the next time it's run
            await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);
        }

        static Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
        {
            // Write details about the error to the console window
            Console.WriteLine($"\tPartition '{ eventArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");
            Console.WriteLine(eventArgs.Exception.Message);
            return Task.CompletedTask;
        }
    }
}