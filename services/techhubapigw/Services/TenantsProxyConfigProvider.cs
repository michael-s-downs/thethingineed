// This code is property of the GGAO // 


using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Primitives;
using Microsoft.ReverseProxy.Abstractions;
using Microsoft.ReverseProxy.Service;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace techhubapigw.Services
{
    public static class InMemoryConfigProviderExtensions
    {
        public static IReverseProxyBuilder LoadFromMemory(this IReverseProxyBuilder builder)
        {
            var provider = new InMemoryConfigProvider(new List<ProxyRoute>(), new List<Cluster>());
            
            builder.Services.AddSingleton<IProxyConfigProvider>(provider);
            builder.Services.AddSingleton<IProxyConfigProviderExtended>(provider);

            return builder;
        }
    }

    public interface IProxyConfigProviderExtended
    {
        void Update(IReadOnlyList<ProxyRoute> routes, IReadOnlyList<Cluster> clusters);
    }

    public class InMemoryConfigProvider : IProxyConfigProvider, IProxyConfigProviderExtended
    {
        private volatile InMemoryConfig _config;
        public IProxyConfig GetConfig() => _config;

        public InMemoryConfigProvider(IReadOnlyList<ProxyRoute> routes, IReadOnlyList<Cluster> clusters)
        {
            _config = new InMemoryConfig(routes, clusters);
        }

        public void Update(IReadOnlyList<ProxyRoute> routes, IReadOnlyList<Cluster> clusters)
        {
            var oldConfig = _config;
            _config = new InMemoryConfig(routes, clusters);
            oldConfig.SignalChange();
        }

        private class InMemoryConfig : IProxyConfig
        {
            private readonly CancellationTokenSource _cts = new CancellationTokenSource();

            public InMemoryConfig(IReadOnlyList<ProxyRoute> routes, IReadOnlyList<Cluster> clusters)
            {
                Routes = routes;
                Clusters = clusters;
                ChangeToken = new CancellationChangeToken(_cts.Token);
            }

            public IReadOnlyList<ProxyRoute> Routes { get; }

            public IReadOnlyList<Cluster> Clusters { get; }

            public IChangeToken ChangeToken { get; }

            internal void SignalChange()
            {
                _cts.Cancel();
            }
        }
    }

    
}
