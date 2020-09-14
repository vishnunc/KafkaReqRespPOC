using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace Transactions.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class TransactionsController : ControllerBase
    {
       
        private readonly ILogger<TransactionsController> _logger;

        public TransactionsController(ILogger<TransactionsController> logger)
        {
            _logger = logger;
        }

        [HttpGet]
        public async Task<string> GetAsync()
        {
            //Just to ensure the server started successfully
            return "true";
        }

        [HttpPost]
        public async Task<JsonElement?> postTransactionAsync([FromBody] JsonElement body)
        {
			//The reply topic name
            string topicName = "reply-some1";

			//Topic creation with AdminClient
			Console.WriteLine("Create Topic Before:" + Timestamp.DateTimeToUnixTimestampMs(DateTime.UtcNow));
			await createTopic(topicName);
			Console.WriteLine("Create Topic After:" + Timestamp.DateTimeToUnixTimestampMs(DateTime.UtcNow));

			//Publish to the topic
			await publish(body.ToString());

			//Consumer config to read from Reply topic
			var conf = new ConsumerConfig
			{
				GroupId = "some",
				BootstrapServers = "localhost:9092",
				AutoOffsetReset = AutoOffsetReset.Earliest,
				//EnableAutoCommit = false,
				SessionTimeoutMs = 6000,
				QueuedMinMessages = 1000000
			};

			//Read from the reply topic
			var consumer = new ConsumerBuilder<Ignore, string>(conf).Build();
			Console.WriteLine("kafka subscribe Topic: {0}, at: {1}", "transactions", DateTime.Now);
			Console.WriteLine("Before Subscribe:" + Timestamp.DateTimeToUnixTimestampMs(DateTime.UtcNow));
			consumer.Assign(new List<TopicPartitionOffset>() { new TopicPartitionOffset(topicName, 0, 0) });
			CancellationTokenSource cts = new CancellationTokenSource();
			cts.CancelAfter(10000);


			try
			{
				bool flag = true;
				while (flag)
				{
					Console.WriteLine("Cosume message at: {0}", DateTime.Now);
					var consumedMessage = consumer.Consume(cts.Token);
					consumer.Commit(consumedMessage);
					flag = false;
					Console.WriteLine("After Consume:" + Timestamp.DateTimeToUnixTimestampMs(DateTime.UtcNow));


				}
			}
			catch (Exception e)
			{
				Console.WriteLine("Failed  While Consuming topic transaction-posted Aggregator Request and Exception: {0} at {1}", e, DateTime.Now);
				Console.WriteLine("Failed While Consumiong Aggregator Request: " + e.Message);
				//consumer.Close();
			}

			//Once the message is read, use Admin client to delete the randomly generate topic
			Console.WriteLine("Delete Topic Before:" + Timestamp.DateTimeToUnixTimestampMs(DateTime.UtcNow));
			await deleteTopic(topicName);

			Console.WriteLine("Delete Topic After:" +Timestamp.DateTimeToUnixTimestampMs(DateTime.UtcNow));
                return body;
            
			
        }

		private async Task deleteTopic(string topicName)
		{


		var adminConfig = new AdminClientConfig { BootstrapServers = "localhost:9092", MaxInFlight = 1 };

		using (var admin = new AdminClientBuilder(adminConfig).Build())
		{
			try
			{
				await admin.DeleteTopicsAsync(new List<string> { topicName });

			}
			catch (CreateTopicsException e)
			{
				Console.WriteLine($"An error occured creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
			}
		}
		}

		

		private async Task createTopic(string topicName)
		{
			var adminConfig = new AdminClientConfig { BootstrapServers = "localhost:9092", MaxInFlight = 1 };

			using (var admin = new AdminClientBuilder(adminConfig).Build())
			{
				try
				{
					await admin.CreateTopicsAsync(new TopicSpecification[] {
						new TopicSpecification { Name = topicName, ReplicationFactor = 1, NumPartitions = 1 } });
				}
				catch (CreateTopicsException e)
				{
					Console.WriteLine($"An error occured creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
				}
			}
    }

		public async 
        Task
publish(String body)
        {
			

			var config = new ProducerConfig { BootstrapServers = "localhost:9092"};
			
			Console.WriteLine("Before Publish:"+Timestamp.DateTimeToUnixTimestampMs(DateTime.UtcNow));
            string somestring = body;//JsonSerializer.Serialize(some);
            using (var p = new ProducerBuilder<string, string>(config).Build())
            {
                try
                {
                   
                    var headers = new Headers();
                    headers.Add("CID", new byte[] { 154});
                    headers.Add("Partition", new byte[] { 1 });
                   
					
                    var dr = await p.ProduceAsync("reply-some1", new Message<string, string> { Key="uid1", Value = somestring , Headers=headers});
                    Console.WriteLine("After Publish:"+Timestamp.DateTimeToUnixTimestampMs(DateTime.UtcNow));
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }
        }
    }
}
