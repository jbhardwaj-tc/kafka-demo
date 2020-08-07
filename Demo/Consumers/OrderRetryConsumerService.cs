using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;

namespace Api.Consumers
{
    public class OrderRetryConsumerService: BackgroundService
    {
        private readonly ProducerConfig _producerConfig;
        private readonly ConsumerConfig _consumerConfig;

        public OrderRetryConsumerService(IOptions<ProducerConfig> producerConfig, IOptions<ConsumerConfig> consumerConfig)
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
                bool someErrorOccured = true;

                var topicName = "test_orders_retry_8";

                var producerWrapper = new ProducerWrapper(_producerConfig, topicName);
                var consumer = new ConsumerWrapper(_consumerConfig, topicName);
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var request = consumer.ReadMessage(stoppingToken);

                        var utcNow = DateTime.UtcNow;
                        var diff = utcNow - request.retryDate.Value.AddSeconds(10);
                        if (diff.Ticks > 0)
                        {
                            Thread.Sleep(diff);
                        }

                        if (someErrorOccured)
                        {
                            await producerWrapper.WriteMessage(request.message, request.retryCount.Value + 1, DateTime.UtcNow);
                            someErrorOccured = false;
                        }

                        consumer.Commit();
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
