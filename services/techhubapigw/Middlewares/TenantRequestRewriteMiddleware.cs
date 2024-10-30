// This code is property of the GGAO // 


using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Threading.Tasks;
using techhubapigw.Auth;
using techhubapigw.Configuration;

namespace techhubapigw.Middlewares
{
    public static class TenantRequestRewriteMiddlewareExtensions
    {
        public static IApplicationBuilder TenantRequestRewrite(
            this IApplicationBuilder builder)
        {
            return builder.UseMiddleware<TenantRequestRewriteMiddleware>();
        }
    }

    public class TenantRequestRewriteMiddleware
    {
        private readonly RequestDelegate _next;
        private readonly string _baseUrl;
        //private readonly UhisConfiguration _options;

        //private readonly ILogger _logger;

        public TenantRequestRewriteMiddleware(
            RequestDelegate next, 
            IConfiguration configuration, 
            IOptions<UhisConfiguration> options
            //ILogger<TenantRequestRewriteMiddleware> logger
            )
        {
            _next = next;
            //_logger = logger;

            var _options = options.Value;

            // Example: http://apigw.uhis-cdac/apigw/license/report/
            _baseUrl = configuration["BaseUrl"];

            
            if (string.IsNullOrWhiteSpace(_baseUrl))
            {
                if (string.IsNullOrEmpty(_options.PrefixNamespace))
                {
                    _baseUrl = $"http://techhubapigw.{_options.Namespace}/apigw/license/report/";
                }
                else
                {
                    _baseUrl = $"http://{_options.PrefixNamespace}-techhubapigw.{_options.Namespace}/apigw/license/report/";
                }
            }
        }

        public async Task Invoke(HttpContext httpContext)
        {
            if (httpContext.Request.Path.StartsWithSegments("/health") ||
                httpContext.Request.Path.StartsWithSegments("/healthcheck") ||
                httpContext.Request.Path.StartsWithSegments("/apigw")
                )
            {
                await _next(httpContext);
                return;
            }

            // No further propagation if not authenticated
            if (!httpContext.User.Identity.IsAuthenticated)
            {
                httpContext.Response.StatusCode = StatusCodes.Status403Forbidden;
                return;
            }

            string tenant = httpContext.User.Identity.Name ?? "unknown";

            if (httpContext.User.HasClaim(ClaimTypes.Role, Roles.Manager) &&
                httpContext.Request.Headers.TryGetValue("x-tenant", out var value))
            {
                tenant = value;
            }
            else
            {
                // Remove faked x-tenant header if exists...
                httpContext.Request.Headers.Remove("x-tenant");

                // Inject header and use it in the proxy when supported in YARP!
                httpContext.Request.Headers.Add("x-tenant", tenant);

                // Inject department header
                httpContext.Request.Headers.Remove("x-department");
                var department = httpContext.User.Claims.FirstOrDefault(c => c.Type == nameof(Database.DTOs.ApiKeyDTO.Department))?.Value;
                if (!string.IsNullOrWhiteSpace(department))
                {
                    httpContext.Request.Headers.Add("x-department", department);
                }
            }

            // Remove the apikey from further requests
            // Remove faked x-tenant header if exists...
            httpContext.Request.Headers.Remove("x-api-key");

            // Inject reporting header to use by the pipelines and extractors
            // WARNING: Full-access users do NOT report usage!
            // httpContext.Request.Headers.Add("x-reporting", $"{_baseUrl}/{httpContext.User.Claims.FirstOrDefault(c => c.Type == nameof(Database.DTOs.ApiKeyDTO.ReportId))?.Value ?? ""}" );
            var reportId = httpContext.User.Claims.FirstOrDefault(c => c.Type == nameof(Database.DTOs.ApiKeyDTO.ReportId))?.Value;
            if (!string.IsNullOrWhiteSpace(reportId)) {
                httpContext.Request.Headers.Add("x-reporting", $"{_baseUrl}{reportId}");
            }

            var currentLimit = httpContext.User.Claims.FirstOrDefault(c => c.Type == nameof(Database.DTOs.ApiKeyDTO.Limits))?.Value;
            if (!string.IsNullOrWhiteSpace(currentLimit)) {
                httpContext.Request.Headers.Add("x-limits", currentLimit);
            }
            // TODO: remove the path rewrite when YARP supports Header matching
            // https://github.com/dotnet/aspnetcore/pull/23594

            // Append the tenant of the ApiKey to allow the reverse proxy to call the correct backend
            httpContext.Request.Path = new PathString($"/{tenant}{httpContext.Request.Path}");

            //_logger.LogInformation($"Url: {httpContext.Request.Path}");

            await _next(httpContext);
        }
    }
}
