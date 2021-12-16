const AWS = require('aws-sdk');
const mongoose = require('mongoose');

class LargeSqs {
    constructor(connection, collection, queueUrl, sqsOptions, mongoTtl) {
        this.sqs = new AWS.SQS(sqsOptions);
        this.queue = queueUrl;
        this.connection = connection;
        this.model = connection.models[collection] || connection.model(collection, new mongoose.Schema({
            payload: Object,
            created_at: Date
        }, {
            autoCreate: true,
            collection
        }));

        // TODO: TTL para apagar msgs que nao foram processadas depois de X tempo.
    }

    async sendMessage(message) {
        // creating mongo record
        const record = await this.model.create({
            payload: message,
            created_at: new Date()
        });

        try {
            // sending mongoId to sqs queue
            const queueResult = await this.sqs.sendMessage({
                MessageBody: JSON.stringify({
                    id: record.id
                }),
                QueueUrl: this.queue
            }).promise();
            return queueResult;
        } catch (error) {
            // remove mongo record if can't send the message
            await this.model.deleteOne({ id: record.id });
            throw error;
        }
    }
}

module.exports = LargeSqs;
