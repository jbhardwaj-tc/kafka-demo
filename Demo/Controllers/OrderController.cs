using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;

namespace Api.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class OrderController : ControllerBase
    {
        private readonly ProducerConfig _config;
        public OrderController(IOptions<ProducerConfig> config)
        {
            this._config = config.Value;
        }

        [HttpGet]
        public IActionResult Get() => Ok();

        [HttpPost]
        public async Task<IActionResult> Post(OrderRequest request)
        {
            var producer = new ProducerWrapper(this._config, "orders");
            var requestJson = JsonConvert.SerializeObject(request);
            return Created("", await producer.WriteMessage(requestJson));
        }
    }
}