// This code is property of the GGAO // 


using Microsoft.AspNetCore.Builder;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Internal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Primitives;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using techhubapigw.Database;
using techhubapigw.Database.DTOs;
using techhubapigw.Models;

namespace techhubapigw.HostedServices
{
    public static class UsageMetricsExtensions
    {
        public static IServiceCollection AddUsageMetrics(this IServiceCollection services)
        {
            services.AddSingleton<IMetricsQueue, MetricsQueue>();
            services.AddHostedService<MetricsConsolidationHostedService>();

            return services;
        }
    }


    public interface IMetricsQueue
    {

        Task PushMetricAsync(UsageReport metric);

        Task<UsageReport> PopMetricAsync();
    }

    public class MetricsQueue: IMetricsQueue
    {
        public ConcurrentQueue<UsageReport> _metrics { get; } = new ConcurrentQueue<UsageReport>();

        public ConcurrentQueue<CancellationTokenSource> waitTokens = new ConcurrentQueue<CancellationTokenSource>();

        public Task<UsageReport> PopMetricAsync()
        {
            _metrics.TryDequeue(out var metric);
            return Task.FromResult(metric);
        }

        public Task PushMetricAsync(UsageReport metric)
        {
            _metrics.Enqueue(metric);

            return Task.CompletedTask;
        }
    }


    public class MetricsConsolidationHostedService : BackgroundService
    {
        private readonly IMetricsQueue _state;
        private readonly IServiceProvider _serviceProvider;
        private readonly IMemoryCache _cache;
        private readonly ISystemClock _systemClock;
        private readonly ILogger _logger;
        Dictionary<string, UsageReport> _reports = new Dictionary<string, UsageReport>();

        public enum Periods { Daily, Weekly, Monthly }
        private Periods _period = Periods.Daily;

        private TimeSpan _saveToDbPeriod = TimeSpan.FromMinutes(5);

        public MetricsConsolidationHostedService(ILogger<MetricsConsolidationHostedService> logger, IMetricsQueue state, IServiceProvider serviceProvider, IMemoryCache cache, ISystemClock systemClock)
        {
            _state = state;
            _serviceProvider = serviceProvider;
            _cache = cache;
            _systemClock = systemClock;

            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {

            await InitalizeDb();

            DateTimeOffset lastDbUpdate = new DateTimeOffset();
            bool newData = false;

            while (!stoppingToken.IsCancellationRequested)
            {
                var metric = await _state.PopMetricAsync();

                if (metric != null)
                {
                    newData = true;

                    var indexId = $"{metric.ReportId}/{metric.Resource}";
                    if (_reports.TryGetValue(indexId, out var currentReport))
                    {
                        currentReport.Count += metric.Count;
                    }
                    else
                    {
                        _reports.Add(indexId, metric);
                    }
                }
                else
                {
                    // Wait for next spin
                    try { await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken); }
                    catch { /* Ignore cancellation token waits exceptions*/ }
                }

                if (lastDbUpdate < _systemClock.UtcNow || stoppingToken.IsCancellationRequested)
                {
                    lastDbUpdate = _systemClock.UtcNow.Add(_saveToDbPeriod);
                    if (newData)
                    {
                        newData = false;

                        _logger.LogInformation("Consolidating to DB");
                        await UpdateMetrics();
                    }
                }
            }
        }

        private async Task UpdateMetrics()
        {
            using (var scope = _serviceProvider.CreateScope())
            {
                var dbContext = scope.ServiceProvider.GetRequiredService<AppDbContext>();

                //await scopedProcessingService.DoWork(stoppingToken);
                foreach (var r in _reports.Values.ToList())
                {
                    await UpdateMetric(dbContext, r);
                }

                // save all changes to DB
                await dbContext.SaveChangesAsync();

                _reports.Clear();
            }
        }

        private async Task UpdateMetric(AppDbContext dbContext, UsageReport report)
        {
            var metric = await GetCurrentPeriodMetric(dbContext, report);
            if (metric != null)
            {
                metric.Count += report.Count;
            }
        }

        private async Task<Metric> GetCurrentPeriodMetric(AppDbContext dbContext, UsageReport report) //string reportId)
        {
            var metricCacheKey = $"metric/{report.ReportId}/{report.Resource}";

            Metric metric = null;

            var foundInCache = _cache.TryGetValue(metricCacheKey, out string metricId);
            var needchange = NeedNextMetric(metric);

            if (!foundInCache || needchange)
            {
                try
                {
                    var apiKey = await GetAndCache<ApiKeyDTO, string>(report.ReportId, e => e.ReportId == report.ReportId, p => p.Key, dbContext);
                    if (!string.IsNullOrWhiteSpace(apiKey))
                    {
                        metric = await dbContext.Metrics
                            .Where(u => u.ApiKeyId == apiKey && u.Resource == report.Resource)
                            .OrderByDescending(u => u.Timestamp)
                            .FirstOrDefaultAsync();

                        if (metric == null || NeedNextMetric(metric))
                        {
                            metric = new Metric
                            {
                                ApiKeyId = apiKey,
                                Timestamp = _systemClock.UtcNow.UtcDateTime,

                                Resource = report.Resource,
                                Count = 0 // Count is added outside!
                            };

                            dbContext.Add(metric);
                        }

                        var cacheEntryOptions = new MemoryCacheEntryOptions().SetSlidingExpiration(TimeSpan.FromMinutes(5));
                        _cache.Set(metricCacheKey, metric, cacheEntryOptions);
                    }

                } catch(Exception e)
                {
                    _logger.LogError(e, "Error obtaining period metric");
                }
            }

            return metric;
        }

        private bool NeedNextMetric(Metric metric)
        {
            if (metric == null) return true;

            var now = _systemClock.UtcNow;

            if (metric.Timestamp.DayOfYear != now.DayOfYear)
            {
                switch (_period)
                {
                    case Periods.Daily: return true;
                    case Periods.Weekly:
                        Calendar calendar = new CultureInfo("en-US").Calendar;
                        if (calendar.GetWeekOfYear(metric.Timestamp, CalendarWeekRule.FirstDay, DayOfWeek.Monday) != calendar.GetWeekOfYear(now.UtcDateTime, CalendarWeekRule.FirstDay, DayOfWeek.Monday))
                        {
                            return true;
                        }
                        break;
                    case Periods.Monthly: return metric.Timestamp.Month != now.Month;
                }
            }

            return false;
        }

        private async Task<T> GetAndCache<T>(AppDbContext dbContext, string indexId, System.Linq.Expressions.Expression<Func<T, bool>> query) where T : class
        {
            string key = $"{typeof(T).Name}/{indexId}";

            if (!_cache.TryGetValue(key, out T obj))
            {
                obj = await dbContext.Set<T>().FirstOrDefaultAsync(query);

                var cacheEntryOptions = new MemoryCacheEntryOptions().SetSlidingExpiration(TimeSpan.FromMinutes(5));
                _cache.Set(key, obj, cacheEntryOptions);
            }

            return obj;
        }

        private async Task<TProperty> GetAndCache<TEntity, TProperty>(string cacheId, System.Linq.Expressions.Expression<Func<TEntity, bool>> query, System.Linq.Expressions.Expression<Func<TEntity, TProperty>> propertySelector, AppDbContext dbContext) where TEntity : class
        {
            string key = $"{typeof(TEntity).Name}/{cacheId}";

            if (!_cache.TryGetValue(key, out TProperty prop))
            {
                var dbObj = await dbContext.Set<TEntity>().FirstOrDefaultAsync(query);

                var cacheEntryOptions = new MemoryCacheEntryOptions().SetSlidingExpiration(TimeSpan.FromMinutes(5));
                if (propertySelector != null)
                {
                    var cache =  propertySelector.Compile().Invoke(dbObj);
                    _cache.Set(key, cache, cacheEntryOptions);
                    return cache;
                }
                else
                {
                    throw new InvalidOperationException("Property selector can't be null");
                }
            }

            return prop;
        }

        private async Task<TEntity> GetOrAddAndCache<TEntity>(string cacheId, System.Linq.Expressions.Expression<Func<TEntity, bool>> query, System.Linq.Expressions.Expression<Func<TEntity>> entityFactory, AppDbContext dbContext) where TEntity : class
        {
            string key = $"{typeof(TEntity).Name}/{cacheId}";

            if (!_cache.TryGetValue(key, out TEntity obj))
            {
                var dbSet = dbContext.Set<TEntity>();

                obj = await dbSet.FirstOrDefaultAsync(query);

                if (obj == null)
                {
                    dbSet.Add(entityFactory.Compile().Invoke());
                }

                var cacheEntryOptions = new MemoryCacheEntryOptions().SetSlidingExpiration(TimeSpan.FromMinutes(5));
                _cache.Set(key, obj, cacheEntryOptions);
            }

            return obj;
        }

        private async Task InitalizeDb()
        {
            _logger.LogInformation("DB Initialization...");
            
            using (var scope = _serviceProvider.CreateScope())
            {
                try
                {
                    var dbContext = scope.ServiceProvider.GetRequiredService<AppDbContext>();
                    //await dbContext.Database.EnsureCreatedAsync();
                    await dbContext.Database.MigrateAsync();

                    // Create first apikey if db is empry
                    var hasData = await dbContext.ApiKeys.AsNoTracking().AnyAsync();
                    if (!hasData)
                    {
                        _logger.LogInformation("DB is empty, seeding...");
                        dbContext.ApiKeys.Add(new ApiKeyDTO { Enabled = true, Key = "initialuhismanager", ReportId = "initialuhismanager", Roles = new List<string>() { Auth.Roles.Manager } });
                        await dbContext.SaveChangesAsync();
                    }

                    _logger.LogInformation("DB ok");
                } catch (Exception e)
                {
                    _logger.LogError(e, "Error initializing DB");
                }
            }
        }
        
    }
}
