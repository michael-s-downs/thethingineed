// This code is property of the GGAO // 


using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using techhubapigw.Database;
using techhubapigw.Database.DTOs;
using techhubapigw.Models;

namespace techhubapigw.Services
{
    public interface ILicenseService
    {
        Task<Tenant> CreateTenant(Tenant tenant);
        Task<Tenant> UpdateTenant(Tenant tenant);

        Task<ApiKey> CreateApiKey(string tenantId, ApiKey apiKey);

        Task<bool> DeleteApiKey(string apiKey);
        Task<ApiKeyDTO> EnableApiKey(string apiKey, bool enable, bool isApiKey);
        Task<ApiKeyDTO> ListApiKey(string apiKey, bool isApiKey);

        Task<ApiKeyDTO> RenewApiKey(string apiKey, bool isApiKey);
        Task<ApiKeyDTO> ResetApiKey(string apiKey, bool isApiKey);

        Task<IEnumerable<Tenant>> ListTenants();
        Task<IEnumerable<Tenant>> ListTenants(bool includeKeys);

        Task ReportUsage(UsageReport report, bool isApiKey);
        Task<IEnumerable<MetricList>> GetMetrics(string tenantId, DateTime start, DateTime end, string? apikey);
    }

    public class LicenseService : ILicenseService
    {
        public IApiKeyService _apiKeyService { get; }

        private readonly AppDbContext _dbContext;
        private readonly ILogger<LicenseService> _logger;
        private readonly ApiKeyServiceState _state;
        private readonly IServiceProvider _serviceProvider;

        private ConcurrentDictionary<string, UsageLimit> _usages => _state.usages;

        public LicenseService(AppDbContext dbContext, IApiKeyService apiKeyService, ApiKeyServiceState state, ILogger<LicenseService> logger, IServiceProvider serviceProvider)
        {
            _apiKeyService = apiKeyService;
            _dbContext = dbContext;
            _state = state;
            _logger = logger;
            _serviceProvider = serviceProvider;
        }

        public async Task<Tenant> CreateTenant(Tenant tenant)
        {
            // Create empty tenant
            tenant.ApiKeys = null;

            // Save to DB
            _dbContext.Tenants.Add(tenant);
            await _dbContext.SaveChangesAsync();

            return tenant;
        }

        public async Task<Tenant> UpdateTenant(Tenant tenant)
        {
            var oldTenant = await _dbContext.Tenants.SingleAsync(t => t.TenantId == tenant.TenantId);

            // Only this properties can be updated
            oldTenant.Name = tenant.Name;
            oldTenant.EntryPoints = tenant.EntryPoints;

            // As the dictionary is compared briefly, we force to update here
            _dbContext.Update(oldTenant);

            await _dbContext.SaveChangesAsync();

            return tenant;
        }

        public async Task<ApiKey> CreateApiKey(string tenantId, ApiKey apikey)
        {
            var ak = new ApiKeyDTO
            {
                Key = Guid.NewGuid().ToString("N"),
                ReportId = apikey.ReportId ?? Guid.NewGuid().ToString("N"),

                Created = DateTime.UtcNow,
                Enabled = true,

                TenantId = tenantId == "NULL" ? null : tenantId,
                Department = apikey.Department,

                Limits = apikey.Limits,
                
                // Beware of this!
                Roles = apikey.Roles
            };

            _dbContext.ApiKeys.Add(ak);
            await _dbContext.SaveChangesAsync();

            apikey.Key = ak.Key;
            apikey.ReportId = ak.ReportId;
            apikey.Created = ak.Created;
            apikey.TenantId = ak.TenantId;
            apikey.Limits = ak.Limits;

            return apikey;
        }

        public Task<IEnumerable<Tenant>> ListTenants() => ListTenants(false);

        public async Task<IEnumerable<Tenant>> ListTenants(bool includeKeys)
        {
            var tenants = includeKeys ? 
                await _dbContext.Tenants.AsNoTracking().Include(t => t.ApiKeys).ToListAsync() :
                await _dbContext.Tenants.AsNoTracking().ToListAsync();
            return tenants;
        }

        public async Task<ApiKeyDTO> EnableApiKey(string apiKey, bool enable, bool isApiKey)
        {
            var ak = await _apiKeyService.EnableApiKey(apiKey, enable, isApiKey);

            _dbContext.Update(ak);

            await _dbContext.SaveChangesAsync();

            return ak;
        }

        public async Task<ApiKeyDTO> ListApiKey(string apiKey, bool isApiKey)
        {
            var ak = await _apiKeyService.Execute(apiKey, isApiKey);
            
            return ak;
        }

        public async Task<ApiKeyDTO> RenewApiKey(string apiKey, bool isApiKey)
        {

            // Old apikey values
            var akOld = await _apiKeyService.Execute(apiKey, isApiKey);

            // Create new apikey
            var ak = new ApiKeyDTO
            {
                Key = Guid.NewGuid().ToString("N"),
                ReportId = Guid.NewGuid().ToString("N"),
                Created = DateTime.UtcNow,
                Enabled = true,
                TenantId = akOld.TenantId,
                Department = akOld.Department,
                Roles = akOld.Roles
            };
            _dbContext.ApiKeys.Add(ak);
            await _dbContext.SaveChangesAsync();

            // Update limits old key with new key
            var lul = await _apiKeyService.GetUsageLimitsByApiKeyAsync(apiKey);
            foreach (var ul in lul)
            {
                ul.ApiKeyId = ak.Key;
            }

            _dbContext.Limits.UpdateRange(lul);

            // Update metrics old key with new key
            var lm = await _apiKeyService.GetMetricsByApiKeyAsync(apiKey);
            foreach (var m in lm)
            {
                m.ApiKeyId = ak.Key;
            }

            _dbContext.Metrics.UpdateRange(lm);

            // Disable old key
            var akOldDisabled = await _apiKeyService.EnableApiKey(apiKey, false, isApiKey);

            _dbContext.ApiKeys.Update(akOldDisabled);
            
            // Write in BBDD all changes
            await _dbContext.SaveChangesAsync();

            // Get new apikey to return
            var akNew = await _apiKeyService.Execute(ak.Key, isApiKey);
            
            return akNew;
        }

        public async Task<ApiKeyDTO> ResetApiKey(string apiKey, bool isApiKey)
        {
           
           var ak = await _apiKeyService.EnableApiKey(apiKey, true, isApiKey);

            _dbContext.Update(ak);
            await _dbContext.SaveChangesAsync();
            
            return ak;
        }

        public async Task<bool> DeleteApiKey(string apiKey)
        {
            var ak = await _dbContext.ApiKeys.FirstOrDefaultAsync(u => u.Key == apiKey);
            
            if (ak == null)
            {
                return false;
            }

            _dbContext.ApiKeys.Remove(ak);
            await _dbContext.SaveChangesAsync();
            return true;
        }

        private async Task<ApiKeyDTO> LoadApiKeyUsages(string reportId, bool isApiKey)
        {
            var ak = await _apiKeyService.GetByReportId(reportId, isApiKey);
            foreach (var us in ak?.Limits)
            {
                _usages.TryAdd(GetUsageIndex(ak.ReportId, us.Resource), us);
            }
            
            return ak;
        }

        public async Task ReportUsage(UsageReport report, bool isApiKey)
        {
           if (!_usages.TryGetValue(GetUsageIndex(report.ReportId, report.Resource), out var usage))
           {

                var ak = await LoadApiKeyUsages(report.ReportId, isApiKey);

                usage = new UsageLimit
                {
                    ApiKeyId = ak.Key,
                    Resource = report.Resource,
                    Current = report.Count,
                    Limit = 0
                };
           }
                     
           usage = _usages.AddOrUpdate(GetUsageIndex(report.ReportId, report.Resource), usage, (key, old) =>
           {
               old.Current += report.Count;
               return old;
           });

           _dbContext.Limits.Update(usage);

           await _dbContext.SaveChangesAsync();
        }

        public async Task<IEnumerable<MetricList>> GetMetrics(string tenantId, DateTime start, DateTime end, string? apikey)
        {
            // Load tenant from DB
            var tenant = await _dbContext.Tenants
                .Include(u => u.ApiKeys)
                .FirstOrDefaultAsync(u => u.TenantId == tenantId);

            var metrics = new List<MetricList>();

            foreach (var ak in tenant?.ApiKeys)
            {   
                // await LoadApiKeyUsages(ak.ReportId);
                if(ak.Key == apikey || string.IsNullOrEmpty(apikey))
                {
                    var mets = await _dbContext.Metrics.Where(u => u.ApiKeyId == ak.Key && u.Timestamp >= start && u.Timestamp <= end).ToListAsync();
                    
                    metrics.AddRange(mets.Select(m => new MetricList
                    {
                        Department = ak.Department,
                        Timestamp = m.Timestamp,
                        Count = m.Count,
                        Resource = m.Resource
                    }));
                }
            }

            //metrics.AddRange(_usages.ToList().Select(u => new MetricList
            //{
            //    Timestamp = DateTime.UtcNow,
            //    Department = tenant?.ApiKeys?.FirstOrDefault(a => a.Key == u.Value.ApiKeyId)?.Department,
            //    Resource = GetResource(u.Key),
            //    Count = u.Value.Current,
            //}));

            // Order by date
            metrics.Sort((a, b) => a.Timestamp.CompareTo(b.Timestamp));

            return metrics;
        }


        // Unified dictionary index
        private string GetUsageIndex(string reportId, string resource) => $"{reportId}/{resource}";

        // Extraction helpers
        private string GetReportId(string usageIndex) => usageIndex.Split('/')[0];
        private string GetResource(string usageIndex) => usageIndex.Split('/')[1];
    }
}
