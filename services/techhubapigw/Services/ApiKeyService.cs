// This code is property of the GGAO // 


using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using techhubapigw.Auth;
using techhubapigw.Database;
using techhubapigw.Database.DTOs;
using techhubapigw.Models;

namespace techhubapigw.Services
{
    public interface IApiKeyService
    {
        Task<ApiKeyDTO> Execute(string providedApiKey);
        Task<ApiKeyDTO> GetByReportId(string reportId);

        Task<ApiKeyDTO> EnableApiKey(string apiKey, bool enabled);

        Task<List<UsageLimit>> GetUsageLimitsByApiKeyAsync(string apiKey);
        Task<List<Metric>> GetMetricsByApiKeyAsync(string apiKey);

    }

    public class ApiKeyService : IApiKeyService
    {
        private readonly ApiKeyServiceState _state;

        private ConcurrentDictionary<string, ApiKeyDTO> _apiKeys => _state.apiKeys;
        private ConcurrentDictionary<string, DateTime> _apiKeysLastAccess => _state.apiKeysLastAccess;

        private ConcurrentDictionary<string, string> _apiKeysByUsage => _state.apiKeysByUsage;
        
        private readonly ILogger _logger;

        private readonly AppDbContext _dbContext;
        
        public ApiKeyService(ILogger<ApiKeyService> logger, AppDbContext dbContext, ApiKeyServiceState state)
        {
            _logger = logger;
            _dbContext = dbContext;
            _state = state;
        }

        public async Task<ApiKeyDTO> Execute(string providedApiKey)
        {
            if (!_apiKeys.TryGetValue(providedApiKey, out var apiKey))
            {
                apiKey = await _dbContext.ApiKeys
                    .Include(e => e.Limits)
                    .FirstOrDefaultAsync(e => e.Key == providedApiKey);

                if (apiKey != null)
                {
                    apiKey = _apiKeys.AddOrUpdate(providedApiKey, apiKey, (providedApiKey, old) => old);

                    if (!string.IsNullOrWhiteSpace(apiKey.ReportId))
                    {
                        _apiKeysByUsage.TryAdd(apiKey.ReportId, providedApiKey);
                    }
                }
            }

            if (apiKey != null)
            {
                _apiKeysLastAccess.AddOrUpdate(providedApiKey, DateTime.UtcNow, (key, old) => DateTime.UtcNow);
            }
            
            return apiKey;
        }

        public async Task<List<UsageLimit>> GetUsageLimitsByApiKeyAsync(string apiKey)
        {
            var lul = await _dbContext.Limits
                .Where(ul => ul.ApiKeyId == apiKey).ToListAsync();

            return lul;
        }

        public async Task<List<Metric>> GetMetricsByApiKeyAsync(string apiKey)
        {
            var lm = await _dbContext.Metrics
                .Where(m => m.ApiKeyId == apiKey).ToListAsync();

            return lm;
        }

        public async Task<ApiKeyDTO> GetByReportId(string reportId)
        {
            if (!_apiKeysByUsage.TryGetValue(reportId, out var key))
            {
                key = (await _dbContext.ApiKeys.FirstOrDefaultAsync(e => e.ReportId == reportId))?.Key; 
            }

            if (!string.IsNullOrWhiteSpace(key))
            {
                var apiKey = await Execute(key);
                return apiKey;
            }

            return null;
        }

        public async Task<ApiKeyDTO> EnableApiKey(string apiKey, bool enabled)
        {
            var ak = await Execute(apiKey);

            if (ak != null)
            {
                ak.Enabled = enabled;
                if (enabled){
                    foreach (var li in ak.Limits){
                        li.Current = 0;
                    }
                }

            }

            return ak;
        }

        public async Task PurgeCache()
        {
            var elements = _apiKeysLastAccess.ToList().OrderBy(e => e.Value);

            var todelete = elements.Take(10).ToList();

            foreach (var el in todelete)
            {
                _apiKeysLastAccess.TryRemove(el.Key, out var _);

                if(_apiKeys.TryRemove(el.Key, out var apiKey))
                {
                    // TODO: persist to database
                    //await _dbContext.ApiKeys.Add(apiKey);
                }
            }
        }
    }
}
