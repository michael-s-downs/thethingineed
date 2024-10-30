// This code is property of the GGAO // 


using Microsoft.Extensions.Hosting;
using Microsoft.ReverseProxy.Service;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using techhubapigw.Database;
using Microsoft.Extensions.DependencyInjection;
using techhubapigw.Services;
using Microsoft.ReverseProxy.Abstractions;
using System.Security.Cryptography;
using Microsoft.Extensions.Logging;
using techhubapigw.Database.DTOs;
using techhubapigw.Configuration;
using Microsoft.Extensions.Options;

namespace techhubapigw.HostedServices
{
    // Extensions
    public static class TenantsMonitorHostedServiceExtensions
    {
        public static IServiceCollection AddTenantRoutingMonitoring(this IServiceCollection services)
        {
            services.AddHostedService<TenantsMonitorHostedService>();
            return services;
        }
    }


    // Implementation
    public class TenantsMonitorHostedService : BackgroundService
    {
        private readonly IProxyConfigProviderExtended _proxyConfigProvider;
        private readonly IOptions<UhisConfiguration> _options;
        private readonly ILogger<TenantsMonitorHostedService> _logger;
        private readonly IServiceProvider _serviceProvider;

        public TenantsMonitorHostedService(ILogger<TenantsMonitorHostedService> logger, IServiceProvider serviceProvider, IProxyConfigProviderExtended proxyConfigProvider, IOptions<UhisConfiguration> options)
        {
            _logger = logger;

            _serviceProvider = serviceProvider;
            _proxyConfigProvider = proxyConfigProvider;

            _options = options;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            SHA1 sha1 = SHA1.Create();
            byte[] hash = null;

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    IEnumerable<Tenant> tenants = null;
                    UhisConfiguration config = null;
                    using (var scope = _serviceProvider.CreateScope())
                    {
                        var options = scope.ServiceProvider.GetRequiredService<IOptions<UhisConfiguration>>();
                        config = options.Value;

                        var licenseService = scope.ServiceProvider.GetRequiredService<ILicenseService>();
                        tenants = await licenseService.ListTenants();
                    }

                    // Compare tenants by hashig theis tenantIds!
                    //var newHash = sha1.ComputeHash(System.Text.UTF8Encoding.UTF8.GetBytes(string.Join("", tenants.Select(t => t.TenantId))));
                    var newHash = sha1.ComputeHash(System.Text.UTF8Encoding.UTF8.GetBytes(
                        string.Join("", tenants.Select(t => t.TenantId + (t.EntryPoints == null ? "" : 
                            string.Join("", t.EntryPoints.ToList().Select(kvp => $"{kvp.Key}{kvp.Value}")))))));

                    if (hash == null || !((ReadOnlySpan<byte>)newHash).SequenceEqual((ReadOnlySpan<byte>)hash))
                    {
                        hash = newHash;

                        _logger.LogInformation("Updating routes for tenants");

                        List<ProxyRoute> routes = new List<ProxyRoute>();
                        List<Cluster> clusters = new List<Cluster>();

                        // Validating entities:
                        using (var scope = _serviceProvider.CreateScope())
                        {
                            var validator = scope.ServiceProvider.GetRequiredService<IConfigValidator>();

                            foreach (var t in tenants)
                            {
                                if (t.EntryPoints == null || !t.EntryPoints.Any())
                                {
                                    t.EntryPoints = _options.Value.DefaultEntryPoints;
                                }

                                // Order route matching as "/" is the last one!
                                var eps = t.EntryPoints.ToList().OrderByDescending(t => t.Key.Length);
                                int c = 0;
                                foreach (var ep in eps)
                                {
                                    c++;

                                    var segment = ep.Key.EndsWith("/") ? ep.Key : $"{ep.Key}/";
                                    var prefixRemoval = ep.Key == "/" ? "" : ep.Key;

                                    // Generate YARP objects
                                    ProxyRoute route = new ProxyRoute
                                    {
                                        RouteId = $"{t.TenantId}-{c}",
                                        ClusterId = $"{t.TenantId}-{c}",
                                        CorsPolicy = "default",
                                        AuthorizationPolicy = Auth.Policies.OnlyThirdParties,
                                        Match =
                                    {
                                        Methods = new[] { "GET", "POST", "PUT", "DELETE" },
                                        Path = $"/{t.TenantId}{segment}{{**catchall}}"
                                    },
                                        // TODO: remove when header routing is available (we will not modify the request path)
                                        Transforms = new List<IDictionary<string, string>>()
                                    {
                                        new Dictionary<string, string>
                                        {
                                            ["PathRemovePrefix"] = $"/{t.TenantId}{prefixRemoval}"
                                        }
                                    }
                                    };

                                    // Timeout
                                    ProxyHttpRequestOptions proxyHttpRequestOptions = new ProxyHttpRequestOptions();
                                    // 10 minutes
                                    proxyHttpRequestOptions.RequestTimeout = TimeSpan.Parse("00:10:00");
                                    // HTTP 1.1 for .NET Core 3.1
                                    // https://docs.microsoft.com/en-us/dotnet/api/system.net.http.httprequestmessage.version?view=netcore-3.1
                                    proxyHttpRequestOptions.Version = Version.Parse("1.1");

                                    string address;

                                    if (string.IsNullOrEmpty(config.PrefixNamespace))
                                    {
                                        address = $"http://{ep.Value}.{t.TenantId}";
                                    }
                                    else
                                    {
                                        address = $"http://{ep.Value}.{config.PrefixNamespace}-{t.TenantId}";
                                    }

                                    Cluster cluster = new Cluster
                                    {
                                        Id = $"{t.TenantId}-{c}",
                                        Destinations =
                                        {
                                            { "proxy", new Destination { Address = address } }
                                        },
                                        HttpRequest = proxyHttpRequestOptions
                                    };

                                    var resultRoute = await validator.ValidateRouteAsync(route);
                                    var resultCluster = await validator.ValidateClusterAsync(cluster);
                                    if (!resultRoute.Any() && !resultCluster.Any())
                                    {
                                        _logger.LogInformation($"Creating route&cluster for: {t.TenantId} => {address}");

                                        routes.Add(route);
                                        clusters.Add(cluster);
                                    }
                                    else
                                    {
                                        foreach (var e in resultRoute) _logger.LogError(e.InnerException, e.Message);
                                        foreach (var e in resultCluster) _logger.LogError(e.InnerException, e.Message);
                                    }
                                }

                                //// Generate YARP objects
                                //ProxyRoute route = new ProxyRoute
                                //{
                                //    RouteId = t.TenantId,
                                //    ClusterId = t.TenantId,
                                //    CorsPolicy = "default",
                                //    AuthorizationPolicy = Auth.Policies.OnlyThirdParties,
                                //    Match =
                                //    {
                                //        Methods = new[] { "GET", "POST", "PUT", "DELETE" },
                                //        Path = $"/{t.TenantId}/{{**catchall}}"
                                //    },
                                //    // TODO: remove when header routing is available (we will not modify the request path)
                                //    Transforms = new List<IDictionary<string, string>>()
                                //    {
                                //        new Dictionary<string, string>
                                //        {
                                //            ["PathRemovePrefix"] = $"/{t.TenantId}"
                                //        }
                                //    }
                                //};

                                //Cluster cluster = new Cluster
                                //{
                                //    Id = t.TenantId,
                                //    Destinations =
                                //    {
                                //        { "proxy", new Destination { Address = $"http://uhisproxy.{config.Namespace}-{t.TenantId}" } }
                                //    }
                                //};

                                //var resultRoute = await validator.ValidateRouteAsync(route);
                                //var resultCluster = await validator.ValidateClusterAsync(cluster);
                                //if (!resultRoute.Any() && !resultCluster.Any())
                                //{
                                //    _logger.LogInformation($"Creating route&cluster for: {t.TenantId} => http://uhisproxy.{config.Namespace}-{t.TenantId}");

                                //    routes.Add(route);
                                //    clusters.Add(cluster);
                                //}
                                //else
                                //{
                                //    foreach (var e in resultRoute) _logger.LogError(e.InnerException, e.Message);
                                //    foreach (var e in resultCluster) _logger.LogError(e.InnerException, e.Message);
                                //}
                            }
                        }

                        _proxyConfigProvider.Update(routes, clusters);

                        _logger.LogInformation("Routes updated");
                    }
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "Error in tenants monitoring");
                }

                await Task.Delay(TimeSpan.FromSeconds(60));
            }

        }
    }
}
