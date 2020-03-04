using System;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Api
{
    public class ProducerWrapper
    {
        private readonly string _topicName;
        private readonly IProducer<string, string> _producer;
        private static readonly Random Rand = new Random();

        public ProducerWrapper(ProducerConfig config, string topicName)
        {
            this._topicName = topicName;
            this._producer = new ProducerBuilder<string, string>(config).Build();
        }
        public async Task<string> WriteMessage(string message)
        {
            var dr = await this._producer.ProduceAsync(this._topicName, new Message<string, string>()
            {
                Key = Rand.Next(5).ToString(),
                Value = message
            });
            return $"KAFKA => Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'";
        }
    }
}
