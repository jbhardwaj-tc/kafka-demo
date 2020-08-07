using System.Linq;
using System.Threading.Tasks;
using Api.Consumers;
using Confluent.Kafka;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Formatters;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Net.Http.Headers;

namespace Api
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddControllers();
            services.Configure<ProducerConfig>(Configuration.GetSection(nameof(ProducerConfig)));
            services.Configure<ConsumerConfig>(Configuration.GetSection(nameof(ConsumerConfig)));
            services.AddHostedService<OrderProcessConsumerService>();
            services.AddHostedService<OrderRetryConsumerService>();
            //services.AddHostedService<OrderConsumerService>();
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            //app.UseHttpsRedirection();

            app.UseRouting();

            app.UseAuthorization();
            //app.Use(async (context, next) =>
            //{
            //    context.Response.OnStarting((state) =>
            //    {
            //        context.Response.ContentType = "application/vnd.kafka.v2+json";
            //        return Task.FromResult(0);
            //    }, null);
            //    await next();
            //});

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
            });
        }
    }
}
