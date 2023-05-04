using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;

namespace Kafka.Producer_API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProducerController : ControllerBase
    {
        private readonly ILogger _logger;

        public ProducerController(ILogger<ProducerController> logger)
        {
            _logger = logger;
        }

        [HttpPost]
        [ProducesResponseType(typeof(string), 201)]
        [ProducesResponseType(400)]
        [ProducesResponseType(500)]
        public async Task<IActionResult> Post([FromQuery] string msg)
        {
            string result = string.Empty;

            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                EnableIdempotence = true
            };

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    //Data is pushed to the broker from the producer and pulled from the broker by the consumer
                    var sendResult = await producer.ProduceAsync("queue_test", new Message<Null, string> { Value = msg });

                    result = $"Message '{sendResult.Value}' of '{sendResult.TopicPartitionOffset}'";
                }
                catch (ProduceException<Null, string> e)
                {
                    _logger.LogError($"Delivery failed: {e.Error.Reason}");
                }
            }

            return Created("", result);
        }
    }
}