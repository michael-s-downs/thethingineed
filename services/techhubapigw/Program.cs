// This code is property of the GGAO // 


using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.Server.Kestrel.Core;

namespace techhubapigw
{
    public class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureAppConfiguration(configurationBuilder =>
                {
                    configurationBuilder.AddJsonFile("/secrets/secret.json", true, true);
                })
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder
                        .UseStartup<Startup>()
                        .ConfigureKestrel(options =>
                        {
                            var maxRequestSizeEnv = long.TryParse(Environment.GetEnvironmentVariable("MAX_REQUEST_SIZE") ?? "31457280", out var maxRequestSize) ? maxRequestSize : 31457280;

                            options.Limits.MaxRequestBodySize = maxRequestSizeEnv;
                        });
                });
    }
}
