using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System.Runtime;
using System.Runtime.Caching;




namespace Company.Function
{
    public static class EventHubTriggerCSharp3
    {
        [FunctionName("EventHubTriggerCSharp3")]
        public static async Task Run([EventHubTrigger("sample-eventhub", Connection = "sampleeventhubnamespacexuhliu_RootManageSharedAccessKey_EVENTHUB")] EventData[] events, ILogger log)
        {
            var exceptions = new List<Exception>();

            int count = 0;
            // Static object in which we will save the cache
            ObjectCache tokenCache = MemoryCache.Default;
            CacheItem tokenContents = null;

            //for each event, aggregate it into local cache
            foreach (EventData eventData in events)
            {
                count ++;

                try
                {
                    string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);

                    //{TODO}somehow cache the event data
                    tokenContents = tokenCache.GetCacheItem("filecontents");
                    tokenContents = new CacheItem("filecontents", messageBody);
                    Console.WriteLine(messageBody);
                    Console.WriteLine(tokenContents);
                    

                    // Replace these two lines with your processing logic.
                    //log.LogInformation($"C# Event Hub trigger function processed a message: {messageBody}"
                    await Task.Yield();
                    
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            //{TODO}if cache has more than 10 messages, process in batch and clear it; otherwise skip;
            if (count % 11 == 0){
             Console.WriteLine("I only process every " + count +" messages!" );
             Console.WriteLine("In cache we have these to process:" + tokenContents.Value as string );
            }


            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }
    }
}
