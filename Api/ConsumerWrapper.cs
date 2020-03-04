using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Api
{
    public class ConsumerWrapper
    {
        private readonly IConsumer<string, string> _consumer;
        public ConsumerWrapper(ConsumerConfig config, string topicName)
        {
            config.AutoOffsetReset = AutoOffsetReset.Earliest;
            this._consumer = new ConsumerBuilder<string, string>(config).Build();
            this._consumer.Subscribe(topicName);
        }
        public string ReadMessage(CancellationToken cancellationToken)
        {
            var consumeResult = this._consumer.Consume(cancellationToken);
            return consumeResult.Value;
        }
    }
}
