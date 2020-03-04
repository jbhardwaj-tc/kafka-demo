using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;

namespace Api.Consumers
{
    public class OrderConsumerService: BackgroundService
    {
        private readonly ProducerConfig _producerConfig;
        private readonly ConsumerConfig _consumerConfig;

        public OrderConsumerService(IOptions<ProducerConfig> producerConfig, IOptions<ConsumerConfig> consumerConfig)
        {
            _producerConfig = producerConfig.Value;
            _consumerConfig = consumerConfig.Value;
        }

        #region Overrides of BackgroundService

        /// <inheritdoc />
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var consumer = new ConsumerWrapper(_consumerConfig, "orders");
                    var request = consumer.ReadMessage(stoppingToken);

                    var orderRequest = JsonConvert.DeserializeObject<OrderRequest>(request);
                    orderRequest.Status = Status.Completed;

                    var producerWrapper = new ProducerWrapper(_producerConfig, "pollrequests");
                    await producerWrapper.WriteMessage(JsonConvert.SerializeObject(orderRequest));
                }
                catch (Exception) { }
            }
        }

        #endregion
    }
}
