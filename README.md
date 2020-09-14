# KafkaReqRespPOC
This is a sample project to perform Request Response pattern in Kafka

# Configuration before running
Before running, please update the launchSettings.json 
Replace "applicationUrl": "https://192.168.1.7:5006;http://192.168.1.7:5007" with the IP of your local machine where you are running the solution
Ensure that Kafka is running locally on 9092 port

# How to run
1. Launch the solution in Visual Studio, start in debug mode
2. Once started, use a tool such as Postman to POST a request to https://<ip of your machine>:5006/transactions
3. The POST request should send a JSON (any JSON is fine)
4. Once posted, the program will respond with the same JSON sent in the request to show the REQ-REPLY pattern works

# To Do
There are some additional configurations and updates to be made to the POC code for actual implementation

1. Fine tune producer and consumer
2. Generate a random topic name (similar to how RabbitMQ generates random queue)
3. Make Create and Delete Topic as separate daemons
