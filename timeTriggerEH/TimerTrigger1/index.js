const { EventHubConsumerClient, earliestEventPosition} = require("@azure/event-hubs");
const { ContainerClient } = require("@azure/storage-blob");    
const { BlobCheckpointStore } = require("@azure/eventhubs-checkpointstore-blob");

const connectionString ="Endpoint=sb://sample-eventhub-namespace-xuhliu.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=UK2TC+aA2ZGXkyfMoNXLzq5qDVJrDxiZdf1OCrNOyJg=;EntityPath=sample-eventhub";
const eventHubName = "sample-eventhub";
const consumerGroup = "$Default"; // name of the default consumer group
const storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=cmob;AccountKey=5/Yh+YVzXOH/+FcdXpjCP/ND4hUyghJxSqrSvq18+tH2A9uTySnLZW4jQD9b1zsBx4/IJhbGMvaP4TTsupNJ+Q==;EndpointSuffix=core.windows.net";
const containerName = "cmob";

module.exports = async function (context, myTimer) {
    const containerClient = new ContainerClient(storageConnectionString, containerName);

    const checkpointStore = new BlobCheckpointStore(containerClient);
    const consumerClient = new EventHubConsumerClient(consumerGroup, connectionString, eventHubName, checkpointStore);

    const subscription = consumerClient.subscribe({
        processEvents: async (events, context) => {
          if (events.length === 0) {
            console.log(`No events received within wait time. Waiting for next interval`);
            return;
          }
  
          for (const event of events) {
            console.log(`Received event: '${event.body}' from partition: '${context.partitionId}' and consumer group: '${context.consumerGroup}'`);
          }
          // Update the checkpoint.
          await context.updateCheckpoint(events[events.length - 1]);
        },
  
        processError: async (err, context) => {
          console.log(`Error : ${err}`);
        }
      }
    );

    await new Promise((resolve) => {
        setTimeout(async () => {
          await subscription.close();
          await consumerClient.close();
          resolve();
        }, 15000);
      });
};

