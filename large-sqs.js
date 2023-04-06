const { SQSClient, SendMessageCommand  } = require('@aws-sdk/client-sqs');
const { Consumer } = require('sqs-consumer');

class LargeSqs {
    constructor(connection, collection, queueUrl, sqsOptions, ttl=1296000) {
        this.sqs = new SQSClient(sqsOptions);
        this.queue = queueUrl;
        this.connection = connection;
        this.model = connection.models[collection] || connection.model(collection, connection.base.Schema({
            payload: Object,
            created_at: { type: Date, expires: ttl }
        }, {
            autoCreate: true,
            collection
        }));
    }

    // Store real message at mongoDb and send only the reccord.id to SQS
    async sendMessage(message) {
        // creating mongo record
        const record = await this.model.create({
            payload: message,
            created_at: new Date()
        });

        try {
            const sendMessageCommand = new SendMessageCommand({
                MessageBody: JSON.stringify({
                    id: record.id
                }),
                QueueUrl: this.queue
            });

            // sending mongoId to sqs queue
            const queueResult = await this.sqs.send(sendMessageCommand)
            return queueResult;
        } catch (error) {
            // remove mongo record if can't send the message
            await this.model.deleteOne({ _id: record.id });
            throw error;
        }
    }

    // Retreive the real message on mongoDB
    async getRecord(message, handleMessage) {
         if(!message || !this.isJsonString(message.Body) || !JSON.parse(message.Body).id) return null;

         // parse SQS message
         const json = JSON.parse(message.Body);

         // recover record from mongodb
         const record = await this.model.findOne({ _id: json.id });
         
         // send record to application :)
         await handleMessage({sqsId: message.MessageId, ...record?.payload});

         // remove record from mongodb
         await this.model.deleteOne({ _id: json.id });
    }

    // Listen SQS queue and retreive the real message on mongoDB
    async listen(sqsConsumerOptions, handleMessage, onError, onProcessingError) {
        const app = Consumer.create({
            queueUrl: this.queue,
            ...sqsConsumerOptions,
            handleMessage: async (message) => {
                return await this.getRecord(message, handleMessage);
            },
            sqs: this.sqs,
        });

        app.on('error', onError);
        app.on('processing_error', onProcessingError);
        app.on('timeout_error', (err, message) => {
            console.log(`LargeSqs => Timeout Error [${err}] [${message.MessageId}]`)
        })    
        app.on('message_received', (message) => {
            console.log(`LargeSqs => Message was received [${message.MessageId}]`)
        })
        app.on('message_processed', (message) => {
            console.log(`LargeSqs => Message was successfully processed and removed from the queue [${message.MessageId}]`)
        })
        app.on('stopped', () => {
            console.log(`LargeSqs => Consumer finally stops its work.`)
        })
        // app.on('empty', () => {
        //     console.log(`LargeSqs => Empty queue, all messages have been consumed`)
        // })
        app.start();
    }

    // This string is a Json ?
    isJsonString(str) {
        try {
            JSON.parse(str);
        } catch (e) {
            return false;
        }
        return true;
    }
}

module.exports = LargeSqs;
