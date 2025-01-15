// This code is property of the GGAO // 


using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using techhubapigw.Auth;
using techhubapigw.Configuration;
using techhubapigw.Database.DTOs;
using techhubapigw.HostedServices;
using techhubapigw.Models;
using techhubapigw.Services;

namespace techhubapigw.Controllers
{
    [Route("apigw/[controller]")]
    [ApiController]
    public class LicenseController : ControllerBase
    {
        private readonly ILicenseService _licenseService;
        private readonly IMetricsQueue _metricsQueue;
        private readonly ILogger<LicenseController> _logger;
        private readonly UhisConfiguration _options;

        public LicenseController(ILogger<LicenseController> logger, ILicenseService licenseService, IMetricsQueue metricsQueue, IOptions<UhisConfiguration> options)
        {
            _licenseService = licenseService;
            _metricsQueue = metricsQueue;
            _logger = logger;
            _options = options.Value;
        }

        [HttpGet("tenants")]
        [Authorize(Roles = Roles.Manager)]
        public async Task<IActionResult> ListTenants(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Listing Tenants");

            var tenants = await _licenseService.ListTenants(true);
            var filtered = tenants.Select(t => new TenantList
            {
                Id = t.TenantId,
                Name = t.Name,

                EntryPoints = t.EntryPoints,

                ApiKeys = t.ApiKeys.Select(ak => new ApiKeyList
                {
                    Key = ak.Key,
                    Department = ak.Department,
                    Roles = ak.Roles
                })
            }).ToList();

            _logger.LogInformation("End listing Tenants");

            return Ok(filtered);
        }

        [HttpPost("tenants")]
        [Authorize(Roles = Roles.Manager)]
        public async Task<IActionResult> NewTenant(CancellationToken cancellationToken, [FromBody] Tenant tenant)
        {
            var tenantId = tenant.TenantId;
            _logger.LogInformation($"Creating Tenant {tenantId}");

            await _licenseService.CreateTenant(tenant);

            _logger.LogInformation($"End creating Tenant {tenantId}");
            
            return Ok(tenant);
        }

        [HttpPut("tenants/{id}")]
        [Authorize(Roles = Roles.Manager)]
        public async Task<IActionResult> UpdateTenant(CancellationToken cancellationToken, string id, [FromBody] Tenant tenant)
        {
            _logger.LogInformation($"Updating Tenant {id}");
            tenant.TenantId = id;

            var ak = await _licenseService.UpdateTenant(tenant);

            _logger.LogInformation($"End updating Tenant {id}");

            return Ok(ak);
        }

        [HttpPost("tenants/{id}/apikeys")]
        [Authorize(Roles = Roles.Manager)]
        public async Task<IActionResult> NewApiKey(CancellationToken cancellationToken, string id, [FromBody] ApiKey apiKey)
        {
            var akDepartment = apiKey.Department;
            _logger.LogInformation($"Creating ApiKey in Department {akDepartment}");

            var ak = await _licenseService.CreateApiKey(id, apiKey);

            _logger.LogInformation($"End creating ApiKey in Department {akDepartment}");

            return Ok(ak);
        }

        [HttpGet("tenants/{id}/apikeys/{akId}/enable")]
        [Authorize(Roles = Roles.Manager)]
        public async Task<IActionResult> EnableApiKey(CancellationToken cancellationToken, string id, string akId)
        {
            _logger.LogInformation($"Enable ApiKey {akId}");

            var ak = await _licenseService.EnableApiKey(akId, true, true);

            _logger.LogInformation($"End enable ApiKey {akId}");

            return Ok(ak);
        }

        [HttpGet("tenants/{id}/apikeys/{akId}/disable")]
        [Authorize(Roles = Roles.Manager)]
        public async Task<IActionResult> DisableApiKey(CancellationToken cancellationToken, string id, string akId)
        {
            _logger.LogInformation($"Disable ApiKey {akId}");

            var ak = await _licenseService.EnableApiKey(akId, false, true);

            _logger.LogInformation($"End disable ApiKey {akId}");

            return Ok(ak);
        }

        [HttpGet("apikey/{id}/list")]
        [Authorize(Roles = Roles.Manager)]
        public async Task<IActionResult> ListApiKey(CancellationToken cancellationToken, string id)
        {
            _logger.LogInformation($"Listing limits of ApiKey: {id}");

            var ak = await _licenseService.ListApiKey(id, true);

            _logger.LogInformation($"End listing limits of ApiKey: {id}");

            return Ok(ak);
        }

        [HttpGet("apikey/{id}/renew")]
        [Authorize(Roles = Roles.Manager)]
        public async Task<IActionResult> RenewApiKey(CancellationToken cancellationToken, string id)
        {
            _logger.LogInformation($"Renew ApiKey: {id}");

            var ak = await _licenseService.RenewApiKey(id, true);

            _logger.LogInformation($"End renew ApiKey: {id}");

            return Ok(ak);
        }

        [HttpGet("apikey/{id}/reset")]
        [Authorize(Roles = Roles.Manager)]
        public async Task<IActionResult> ResetApiKey(CancellationToken cancellationToken, string id)
        {
            _logger.LogInformation($"Reset ApiKey: {id}");

            var ak = await _licenseService.ResetApiKey(id, true);

            _logger.LogInformation($"End reset of ApiKey: {id}");

            return Ok(ak);
        }

        [HttpPost("report/{id}")]
        //[Authorize(Roles = Roles.ThirdParty)]
        public async Task<IActionResult> Message(CancellationToken cancellationToken, string id, [FromBody] UsageReport report)
        {
            _logger.LogInformation($"Reporting to Id: {id}");
            report.ReportId = id;

            // view what host is used to limit posting from outside of the cluster
            _logger.LogInformation($"Host: {report.ReportId}");

            // Only insert metrics if called from inside the cluster (yeah, thats a little permissive/fakeable)
            if (this.Request.Host.Host.EndsWith(_options.Namespace))
            {
                await _licenseService.ReportUsage(report, true);
                await _metricsQueue.PushMetricAsync(report);
            }

            // Do not return any hint to an attacker!
            _logger.LogInformation($"End to reporting to Id: {id}");

            return Ok();
        }

        [HttpGet("report/{id}/list")]
        public async Task<IActionResult> ListApiKeyByReporId(CancellationToken cancellationToken, string id)
        {
            _logger.LogInformation($"Listing apikey of ReportId: {id}");

            var ak = await _licenseService.ListApiKey(id, false);

            _logger.LogInformation($"End listing apikey of ReportId: {id}");

            return Ok(ak);
        }

        [HttpGet("metrics/{id}")]
        [Authorize(Roles = Roles.Metrics)]
        public async Task<IActionResult> Metrics(CancellationToken cancellationToken, string id, [FromQuery] DateTime? start, [FromQuery] DateTime? end, [FromQuery] string? apikey)
        {
            // Read current usage metrics
            _logger.LogInformation("Reporting metrics");
            if (!start.HasValue)
            {
                start = DateTime.UtcNow.Subtract(TimeSpan.FromDays(1));
            }
            if (!end.HasValue)
            {
                end = DateTime.UtcNow.Add(TimeSpan.FromDays(1));
            }

            var metrics = await _licenseService.GetMetrics(id, start.Value, end.Value, apikey);

            _logger.LogInformation("End to reporting metrics");

            return Ok(metrics);
        }
    }
}
