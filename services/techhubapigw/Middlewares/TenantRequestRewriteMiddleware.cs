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
using techhubapigw.Cors;

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
        private readonly CorsSettings _corsSettings;
        private readonly UhisConfiguration _options;
        private readonly ILogger _logger;

        public TenantRequestRewriteMiddleware(
            RequestDelegate next, 
            IConfiguration configuration, 
            IOptions<UhisConfiguration> options,
            ILogger<TenantRequestRewriteMiddleware> logger
        )
        {
            _next = next;
            _logger = logger;
            _corsSettings = configuration.GetSection("CorsSettings").Get<CorsSettings>();
            _options = options.Value;
            _baseUrl = configuration["BaseUrl"];

            // Si _baseUrl no está configurado, se establece un valor por defecto en función de los prefijos
            if (string.IsNullOrWhiteSpace(_baseUrl))
            {
                _baseUrl = !string.IsNullOrEmpty(_options.PrefixNamespace)
                    ? $"http://{_options.PrefixNamespace}-techhubapigw.{_options.Namespace}/apigw/license/report/"
                    : $"http://techhubapigw.{_options.Namespace}/apigw/license/report/";
            }
        }

        public async Task Invoke(HttpContext httpContext)
        {
            // Verificación rápida para /health, /healthcheck y /apigw
            if (IsHealthCheckPath(httpContext))
            {
                await _next(httpContext);
                return;
            }

            // Manejo de solicitudes OPTIONS para CORS
            if (httpContext.Request.Method == "OPTIONS")
            {
                HandleCorsPreflight(httpContext);
                return;
            }

            // Si el usuario no está autenticado, responde con 403 Forbidden
            if (!httpContext.User.Identity.IsAuthenticated)
            {
                httpContext.Response.StatusCode = StatusCodes.Status403Forbidden;
                return;
            }

            // Agregar encabezados personalizados basados en la autenticación y roles
            SetTenantAndDepartmentHeaders(httpContext);
            SetReportingHeaders(httpContext);

            // Reescribe la ruta con el prefijo de inquilino (tenant) para el proxy inverso
            var tenant = httpContext.User.Identity.Name ?? "unknown";
            httpContext.Request.Path = new PathString($"/{tenant}{httpContext.Request.Path}");

            // Configura los encabezados CORS para la respuesta
            SetCorsHeaders(httpContext);

            await _next(httpContext);
        }

        private bool IsHealthCheckPath(HttpContext httpContext) =>
            httpContext.Request.Path.StartsWithSegments("/health") ||
            httpContext.Request.Path.StartsWithSegments("/healthcheck") ||
            httpContext.Request.Path.StartsWithSegments("/apigw");

        private void HandleCorsPreflight(HttpContext httpContext)
        {
            string origin = httpContext.Request.Headers["Origin"];
            _logger.LogInformation("Processing OPTIONS request from origin: {origin}", origin);

            if (!string.IsNullOrEmpty(origin) && IsOriginAllowed(origin))
            {
                _logger.LogInformation("Processing with allowed origin: {origin}", origin);
                httpContext.Response.Headers.Add("Access-Control-Allow-Origin", origin);
            }
            else
            {
                _logger.LogWarning("Origin {origin} is not allowed", origin);
                httpContext.Response.Headers.Add("Access-Control-Allow-Origin", "*");
            }

            AddCorsHeaders(httpContext, true);
            httpContext.Response.StatusCode = StatusCodes.Status200OK;
        }

        private void SetTenantAndDepartmentHeaders(HttpContext httpContext)
        {
            var tenant = httpContext.User.Identity.Name ?? "unknown";

            if (httpContext.User.HasClaim(ClaimTypes.Role, Roles.Manager) &&
                httpContext.Request.Headers.TryGetValue("x-tenant", out var value))
            {
                tenant = value;
            }
            else
            {
                httpContext.Request.Headers.Remove("x-tenant");
                httpContext.Request.Headers.Add("x-tenant", tenant);

                // Configura el encabezado x-department si el usuario tiene dicho reclamo
                httpContext.Request.Headers.Remove("x-department");
                var department = httpContext.User.Claims.FirstOrDefault(c => c.Type == nameof(Database.DTOs.ApiKeyDTO.Department))?.Value;
                if (!string.IsNullOrWhiteSpace(department))
                {
                    httpContext.Request.Headers.Add("x-department", department);
                }
            }

            // Elimina el encabezado x-api-key para mayor seguridad
            httpContext.Request.Headers.Remove("x-api-key");
        }

        private void SetReportingHeaders(HttpContext httpContext)
        {
            var reportId = httpContext.User.Claims.FirstOrDefault(c => c.Type == nameof(Database.DTOs.ApiKeyDTO.ReportId))?.Value;
            if (!string.IsNullOrWhiteSpace(reportId))
            {
                httpContext.Request.Headers.Add("x-reporting", $"{_baseUrl}{reportId}");
            }

            var currentLimit = httpContext.User.Claims.FirstOrDefault(c => c.Type == nameof(Database.DTOs.ApiKeyDTO.Limits))?.Value;
            if (!string.IsNullOrWhiteSpace(currentLimit))
            {
                httpContext.Request.Headers.Add("x-limits", currentLimit);
            }
        }

        private void SetCorsHeaders(HttpContext httpContext)
        {
            var origin = httpContext.Request.Headers["Origin"];
            if (IsOriginAllowed(origin))
            {
                httpContext.Response.Headers.Add("Access-Control-Allow-Origin", origin);
                AddCorsHeaders(httpContext, true);
            }
            else
            {
                httpContext.Response.Headers.Add("Access-Control-Allow-Origin", "*");
                AddCorsHeaders(httpContext, false);
            }
        }

        private void AddCorsHeaders(HttpContext httpContext, bool addCredentials)
        {
            httpContext.Response.Headers.Add("Access-Control-Allow-Methods", string.Join(", ", _corsSettings.AllowedMethod));
            httpContext.Response.Headers.Add("Access-Control-Allow-Headers", string.Join(", ", _corsSettings.AllowedHeader));
            httpContext.Response.Headers.Add("Access-Control-Max-Age", _corsSettings.MaxAge);
            if(addCredentials){
                httpContext.Response.Headers.Add("Access-Control-Allow-Credentials", _corsSettings.AllowCredentials.ToString().ToLower());
            }
        }

        private bool IsOriginAllowed(string origin)
        {
            if (_corsSettings.AllowedOrigins == null || !_corsSettings.AllowedOrigins.Any() || origin == null)
            {
                _logger.LogWarning("Allowed origins list is empty or null.");
                return false;
            }

            _logger.LogInformation("List without modify: {originalList}", string.Join(", ",  _corsSettings.AllowedOrigins));
            List<string> modifiedList = _corsSettings.AllowedOrigins
            .Select(item => item.Replace("https://*.", "https://"))
            .ToList();
            _logger.LogInformation("Modified allowed origins list: {modifiedList}", string.Join(", ", modifiedList));

            foreach (var checkingOrigin in modifiedList)
            {
                _logger.LogInformation("Checking with allowed origin pattern: {checkingOrigin}", checkingOrigin);

                if (checkingOrigin.Equals(origin, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }
            return false;
        }
    }
}
