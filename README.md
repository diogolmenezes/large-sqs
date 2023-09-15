# LargeSQS

This lib helps to send larger messages to AWS SQS queue.

The maximum size of SQS message is 256 kbytes, so this lib will receave your
larger payload, store in mongoDB collection and send to SQL only mongodb id. This way you can have big messages in your application.

