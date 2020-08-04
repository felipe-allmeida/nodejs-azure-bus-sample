const { ServiceBusClient, ReceiveMode } = require('@azure/service-bus');

require('dotenv').config();

async function main() {
    const sbClient = ServiceBusClient.createFromConnectionString(process.env.AZURE_BUS_CONNECTION_STRING);    

    // If receiving from a Subscription, use `createSubscriptionClient` instead of `createQueueClient`
    const queueClient = sbClient.createQueueClient(process.env.QUEUE_NAME);
    
    // controls weather we continue to recover from fatal Receiver failures
    let enableReceiverRecovery = true;

    console.log(`Receiver started and ready to receive from ${process.env.QUEUE_NAME}`);

    do {
        // PeekLock mode receives the message then confirm it has been received.
        // ReceiveAndDelete receive and deletes the message, without confirming the it has been received.
        // It is most performatic but we have the risk of losing messages.
        // https://docs.microsoft.com/en-us/azure/service-bus-messaging/service-bus-performance-improvements?tabs=net-standard-sdk#receive-mode
        const receiver = queueClient.createReceiver(ReceiveMode.peekLock);

        const receiverPromise = new Promise((resolve, __reject) => {
            const onMessageHandler = async (brokeredMessage) => {
                console.log(`Received message: ${brokeredMessage.body}`);                
                await brokeredMessage.complete();
            };

            const onErrorHandler = (err) => {
                if (err.retryable === true) {
                    console.log(`Receiver will be recreated. A recoverable error ocurred: ${err}`);
                    resolve();
                } else {
                    console.log(`$Error ocurred: ${err}`);
                }
            };
    
            receiver.registerMessageHandler(onMessageHandler, onErrorHandler, { autoComplete: false });
        });

        // this will only resolve if our receiver has failed in a way that is not recoverable.
        await receiverPromise;

        // the Service Bus package is intended to be resilient in the face of transitive issues, like network
        // interruptions. If there are continual restarts this might indicate a more serious issue, like a network
        // outage.
        console.log(`Closing previous receiver and recreating - a fatal error has ocurred.`);

        // close the old receiver and just let the loop start again
        await receiver.close();
    } while(enableReceiverRecovery);

    await queueClient.close();
    await sbClient.close();

}

main().catch((err) => {
    console.log(`Error occurred: ${err}`);
});