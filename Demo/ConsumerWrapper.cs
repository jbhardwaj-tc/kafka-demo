using System;
using System.Text;
using System.Threading;
using Confluent.Kafka;

namespace Api
{
    public class ConsumerWrapper
    {
        private readonly IConsumer<string, string> _consumer;
        public ConsumerWrapper(ConsumerConfig config, string topicName)
        {
            config.AutoOffsetReset = AutoOffsetReset.Earliest;
            config.EnableAutoCommit = false;
            this._consumer = new ConsumerBuilder<string, string>(config).Build();
            this._consumer.Subscribe(topicName);
        }
        public (string message, int? retryCount, DateTime? retryDate, TopicPartitionOffset topicPartitionOffset) ReadMessage(CancellationToken cancellationToken)
        {
            var consumeResult = this._consumer.Consume(cancellationToken);

            int? retryCount = null;

            if (consumeResult.Headers.TryGetLastBytes("retry_count", out byte[] countBytes))
            {
                retryCount = int.Parse(Encoding.UTF8.GetString(countBytes));
            }

            DateTime? retryDate = null;

            if (consumeResult.Headers.TryGetLastBytes("retry_date", out byte[] dateBytes))
            {
                retryDate = DateTime.Parse(Encoding.UTF8.GetString(dateBytes));
            }

            return (consumeResult.Value, retryCount, retryDate, consumeResult.TopicPartitionOffset);
        }

        public void Commit()
        {
            _consumer.Commit();
        }

        public void Seek(TopicPartitionOffset topicPartitionOffset)
        {
            _consumer.Seek(topicPartitionOffset);
        }
    }
}