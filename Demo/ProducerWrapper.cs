using System;
using System.Text;
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
        public async Task<string> WriteMessage(string message, int? retryCount = null, DateTime? retryDate = null)
        {
            var kafkaMessage = new Message<string, string>()
            {
                Key = Rand.Next(5).ToString(),
                Value = message,
                Headers = new Headers()
            };



            if (retryCount.HasValue)
            {
                kafkaMessage.Headers.Add("retry_count", Encoding.UTF8.GetBytes(retryCount.Value.ToString()));
            }

            if (retryDate.HasValue)
            {
                kafkaMessage.Headers.Add("retry_date", Encoding.UTF8.GetBytes(retryDate.ToString()));
            }

            var dr = await this._producer.ProduceAsync(this._topicName, kafkaMessage);
            return $"KAFKA => Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'";
        }
    }
}
