using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;

namespace Api.Consumers
{
    public class OrderProcessConsumerService: BackgroundService
    {
        private readonly ProducerConfig _producerConfig;
        private readonly ConsumerConfig _consumerConfig;

        public OrderProcessConsumerService(IOptions<ProducerConfig> producerConfig, IOptions<ConsumerConfig> consumerConfig)
        {
            _producerConfig = producerConfig.Value;
            _consumerConfig = consumerConfig.Value;
        }

        #region Overrides of BackgroundService

        /// <inheritdoc />
        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            return Task.Factory.StartNew(async () =>
            {
                bool databaseExceptionOccured = true;


                var retryPoducerWrapper = new ProducerWrapper(_producerConfig, "test_orders_retry_8");

                var topicName = "test_orders_8";

                var producerWrapper = new ProducerWrapper(_producerConfig, topicName);
                await producerWrapper.WriteMessage(JsonConvert.SerializeObject(new OrderRequest
                {
                    OrderId = 1,
                    Price = 2,
                    ProductName = "Test" + DateTime.Now.ToString(),
                    Status = Status.Submitted
                }));

                var consumer = new ConsumerWrapper(_consumerConfig, topicName);
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var request = consumer.ReadMessage(stoppingToken);

                        // If occurs some exception with working with Digicert
                        if (databaseExceptionOccured) {
                            consumer.Seek(request.topicPartitionOffset);
                            databaseExceptionOccured = false;
                        }
                        else
                        {
                            await retryPoducerWrapper.WriteMessage(request.message, 1, DateTime.UtcNow);
                            consumer.Commit();
                        }
                    }
                    catch (Exception exception)
                    {
                    }
                }
            });
        }

        #endregion
    }
}
