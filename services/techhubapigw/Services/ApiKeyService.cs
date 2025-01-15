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
        Task<ApiKeyDTO> Execute(string providedValue, bool isApiKey);
        Task<ApiKeyDTO> GetByReportId(string reportId, bool isApiKey);

        Task<ApiKeyDTO> EnableApiKey(string apiKey, bool enabled, bool isApiKey);

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

        public async Task<ApiKeyDTO> Execute(string providedValue, bool isApiKey)
        {
            ApiKeyDTO apiKey = new ApiKeyDTO();

            if(isApiKey)
            {
                apiKey = await GetCurrentApiKeyValues(providedValue);
            }
            else
            {
                string apiKeyCached;

                if (!_apiKeysByUsage.TryGetValue(providedValue, out apiKeyCached))
                {
                    apiKey = await _dbContext.ApiKeys
                            .Include(e => e.Limits)
                            .FirstOrDefaultAsync(e => e.ReportId == providedValue);
                    
                    apiKeyCached = apiKey.Key;
                    
                }
                
                apiKey = await GetCurrentApiKeyValues(apiKeyCached);
                
            }

            // Update last access to dictionary in memory
            if (apiKey != null)
            {
                _apiKeysLastAccess.AddOrUpdate(providedValue, DateTime.UtcNow, (key, old) => DateTime.UtcNow);
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

        public async Task<ApiKeyDTO> GetByReportId(string reportId, bool isApiKey)
        {
            if (!_apiKeysByUsage.TryGetValue(reportId, out var key))
            {
                key = (await _dbContext.ApiKeys.FirstOrDefaultAsync(e => e.ReportId == reportId))?.Key; 
            }

            if (!string.IsNullOrWhiteSpace(key))
            {
                var apiKey = await Execute(key, isApiKey);
                return apiKey;
            }

            return null;
        }

        public async Task<ApiKeyDTO> EnableApiKey(string apiKey, bool enabled, bool isApiKey)
        {
            var ak = await Execute(apiKey, isApiKey);

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

        private async Task<ApiKeyDTO> GetCurrentApiKeyValues(string providedValue)
        {
            ApiKeyDTO apiKey;
            // Try recover apikey of dictionary in memory
            if (!_apiKeys.TryGetValue(providedValue, out apiKey))
            {
                // Don´t exist in dictionary in memory then recover of BBDD
                apiKey = await _dbContext.ApiKeys
                    .Include(e => e.Limits)
                    .FirstOrDefaultAsync(e => e.Key == providedValue);

                // If exists apikey
                if (apiKey != null)
                {   
                    // Add to dictionary in memory or keep if exists
                    apiKey = _apiKeys.AddOrUpdate(providedValue, apiKey, (providedValue, old) => old);

                    // If apikey has reportId
                    if (!string.IsNullOrWhiteSpace(apiKey.ReportId))
                    {
                        // Add to dictionary in memory of usage with report id as key
                        _apiKeysByUsage.TryAdd(apiKey.ReportId, providedValue);
                    }
                }
            }

            return apiKey;
        }
    }
}
