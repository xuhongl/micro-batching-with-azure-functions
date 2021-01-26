const df = require("durable-functions");

module.exports = async function (context, req) {
    const client = df.getClient(context);

    //use client.startNew to star a new orchestration
    const instanceId = await client.startNew(req.params.functionName, undefined, req.body);

    context.log(`Started orchestration with ID = '${instanceId}'.`);

    //Then it uses client.createCheckStatusResponse to return an HTTP response containing URLs that can be used to monitor and manage the new orchestration.
    return client.createCheckStatusResponse(context.bindingData.req, instanceId);
};


// module.exports = async function (context) {
//     const client = df.getClient(context);
//     const entityId = new df.EntityId("Counter", "myCounter");
//     await context.df.signalEntity(entityId, "add", 1);
// };