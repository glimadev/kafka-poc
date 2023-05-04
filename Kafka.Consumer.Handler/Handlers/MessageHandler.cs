using Confluent.Kafka;

namespace Kafka.Consumer.Handler.Handlers
{
    public class MessageHandler : BackgroundService
    {
        private readonly ILogger _logger;

        public MessageHandler(ILogger<MessageHandler> logger)
        {
            _logger = logger;
        }

        /// <summary>
        /// The Kafka consumer works by issuing "fetch" requests to the brokers leading 
        /// the partitions it wants to consume. The consumer specifies its offset in the 
        /// log with each request and receives back a chunk of log beginning from that position. 
        /// The consumer thus has significant control over this position and can rewind it to 
        /// re-consume data if need be.
        /// </summary>
        /// <param name="stoppingToken"></param>
        /// <returns></returns>
        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var conf = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
                EnableAutoOffsetStore = false
            };

            using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                c.Subscribe("queue_test");

                var cts = new CancellationTokenSource();

                try
                {
                    while (!cts.IsCancellationRequested)
                    {
                        var message = c.Consume(cts.Token);

                        _logger.LogInformation($"Message: {message.Value} received {message.TopicPartitionOffset}");

                        c.Commit(message);

                        c.StoreOffset(message);
                    }
                }
                catch (OperationCanceledException)
                {
                    c.Close();
                }
            }

            return Task.CompletedTask;
        }
    }
}
