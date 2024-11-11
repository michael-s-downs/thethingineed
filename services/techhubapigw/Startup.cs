// This code is property of the GGAO // 


using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Internal;
using Pomelo.EntityFrameworkCore.MySql.Infrastructure;
using techhubapigw.Auth;
using techhubapigw.Auth.Handlers;
using techhubapigw.Auth.Requirements;
using techhubapigw.Configuration;
using techhubapigw.Database;
using techhubapigw.HostedServices;
using techhubapigw.Middlewares;
using techhubapigw.Models;
using techhubapigw.Services;
using techhubapigw.Cors;

namespace techhubapigw
{
    public class Startup
    {
        public IConfiguration Configuration { get; }

        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        // This method gets called by the runtime. Use this method to add services to the container.
        // For more information on how to configure your application, visit https://go.microsoft.com/fwlink/?LinkID=398940
        public void ConfigureServices(IServiceCollection services)
        {
            services.TryAddSingleton<ISystemClock, SystemClock>();
            services.AddMemoryCache();

            // Configuration
            services.Configure<UhisConfiguration>(Configuration.GetSection(UhisConfiguration.Section));

            // States
            services.AddSingleton<ApiKeyServiceState>();

            // Services
            services.AddScoped<IApiKeyService, ApiKeyService>();
            services.AddScoped<ILicenseService, LicenseService>();

            services.AddUsageMetrics();
            services.AddTenantRoutingMonitoring();

            // Enable CORS (full access)
            services.AddCors(options =>
            {
                options.AddPolicy("default", builder =>
                {
                    var corsSettings = Configuration.GetSection("CorsSettings").Get<CorsSettings>();

                    builder.WithOrigins(corsSettings.AllowedOrigins.ToArray())
                        .WithMethods(corsSettings.AllowAnyMethod ? "GET, POST, PUT, DELETE, OPTIONS" : "")
                        .WithHeaders(corsSettings.AllowAnyHeader ? "Content-Type, Authorization, X-Api-Key" : "")
                        .AllowCredentials();
                });
            });

            // Configure Kestrel to allow up to 30 MB requests
            services.Configure<KestrelServerOptions>(options =>
            {
                options.Limits.MaxRequestBodySize = 30 * 1024 * 1024; // 30 MB
            });

            // Configure IIS (if applicable)
            services.Configure<IISServerOptions>(options =>
            {
                options.MaxRequestBodySize = 30 * 1024 * 1024; // 30 MB
            });

            // AUTH
            services.AddAuthentication(options =>
                {
                    options.DefaultAuthenticateScheme = ApiKeyAuthenticationOptions.DefaultScheme;
                    options.DefaultChallengeScheme = ApiKeyAuthenticationOptions.DefaultScheme;
                })
                .AddApiKeySupport(options => { });

            // TODO: use some reflection magic??
            services.AddAuthorization(options =>
            {
                // Add all policies
                options.AddPolicy(Policies.OnlyTrainers, policy => policy.Requirements.Add(new OnlyTrainersRequirement()));
                options.AddPolicy(Policies.OnlyThirdParties, policy => policy.Requirements.Add(new OnlyThirdPartiesRequirement()));
            });

            // Inject the handlers
            services.AddSingleton<IAuthorizationHandler, OnlyThirdPartiesAuthorizationHandler>();
            services.AddSingleton<IAuthorizationHandler, OnlyThirdPartiesAuthorizationHandler>();

            // DAL: to create a migration => dotnet-ef migrations add Init --project techhubapigw\techhubapigw.csproj
            string connectionString = File.ReadAllText(Configuration.GetConnectionString("sqldb"));
            services.AddDbContextPool<AppDbContext>(options => options
                //.UseMySql("Server=localhost;Database=uhis;User=root;Password=1234;", mySqlOptions => mySqlOptions /*replace with your Server Version and Type*/.ServerVersion(new Version(8, 0, 21), ServerType.MySql)
                //"Server=localhost;Database=uhis;User=root;Password=1234;")
                .UseMySql(connectionString, builder => 
                {
                    builder.EnableRetryOnFailure(5, TimeSpan.FromSeconds(2), null);
                })
                //.UseInMemoryDatabase("uhis")
            );

            // HealthChecks
            services.AddHealthChecks();

            // Controllers
            services.AddControllers();

            // PROXY
            services.AddReverseProxy()
                //.LoadFromConfig(Configuration.GetSection("ReverseProxy"))
                .LoadFromMemory();
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseAuthentication();

            // Rewrite request to call the apropriate tenant services
            app.TenantRequestRewrite();
            app.UseRouting();

            app.UseCors("default");

            app.UseAuthorization();

            // TODO: License checking middleware
            // app.UseLicense();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapHealthChecks("/health");
                endpoints.MapControllers();
                endpoints.MapReverseProxy();
            });
        }
    }
}
