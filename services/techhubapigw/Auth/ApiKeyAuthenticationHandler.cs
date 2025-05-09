// This code is property of the GGAO // 


using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Threading.Tasks;
using techhubapigw.Auth.Problems;
using techhubapigw.Services;

namespace techhubapigw.Auth
{
    // Following this guide:
    // https://josef.codes/asp-net-core-protect-your-api-with-api-keys/

    public class ApiKeyAuthenticationHandler : AuthenticationHandler<ApiKeyAuthenticationOptions>
    {
        private const string ProblemDetailsContentType = "application/problem+json";
        private readonly IApiKeyService _getApiKeyQuery;
        private const string ApiKeyHeaderName = "x-api-key";
        private const string AuthorizationName = "Authorization";

        //private string _baseUrl;

        public ApiKeyAuthenticationHandler(
            IOptionsMonitor<ApiKeyAuthenticationOptions> options,
            //IConfiguration configution,
            ILoggerFactory logger,
            UrlEncoder encoder,
            ISystemClock clock,
            IApiKeyService getApiKeyService) : base(options, logger, encoder, clock)
        {
            //_baseUrl = configution["BaseUrl"];
            _getApiKeyQuery = getApiKeyService ?? throw new ArgumentNullException(nameof(getApiKeyService));
        }

        protected override async Task<AuthenticateResult> HandleAuthenticateAsync()
        {
            string providedApiKey = null;

            if (!Request.Headers.TryGetValue(AuthorizationName, out var authorizationHeaderValues))
            {
                if (!Request.Headers.TryGetValue(ApiKeyHeaderName, out var apiKeyHeaderValues))
                {
                    return AuthenticateResult.NoResult();
                }

                providedApiKey = apiKeyHeaderValues.FirstOrDefault();

                if (apiKeyHeaderValues.Count == 0 || string.IsNullOrWhiteSpace(providedApiKey))
                {
                    return AuthenticateResult.NoResult();
                }
            }
            else
            {
                providedApiKey = authorizationHeaderValues.FirstOrDefault();

                if (authorizationHeaderValues.Count == 0 || string.IsNullOrWhiteSpace(providedApiKey))
                {
                    return AuthenticateResult.NoResult();
                }
                
            }
            
            var existingApiKey = await _getApiKeyQuery.Execute(providedApiKey, true);

            if (existingApiKey != null && existingApiKey.Enabled)
            {
                Dictionary<string, Dictionary<string, int>> transformedDictionary = existingApiKey.Limits
                    .Where(item => item.Limit != 0)
                    .ToDictionary(
                        item => item.Resource,
                        item => new Dictionary<string, int>{
                            { "Current", item.Current },
                            { "Limit", item.Limit }
                        });

                var currentLimit = JsonSerializer.Serialize(transformedDictionary);
                var claims = new List<Claim>
                {
                    // Using owner to match the correct tenant backend
                    new Claim(ClaimTypes.Name, existingApiKey.TenantId ?? ""),
                    // Used to receive metrics reporting
                    new Claim(nameof(Database.DTOs.ApiKeyDTO.ReportId), existingApiKey.ReportId ?? ""), //$"{_baseUrl}/{existingApiKey.ReportId}"),
                    // Adding the department info
                    new Claim(nameof(Database.DTOs.ApiKeyDTO.Department), existingApiKey.Department ?? ""),
                    // Adding the limits info
                    new Claim(nameof(Database.DTOs.ApiKeyDTO.Limits), currentLimit ?? "")
                };

                claims.AddRange(existingApiKey.Roles.Select(role => new Claim(ClaimTypes.Role, role)));

                var identity = new ClaimsIdentity(claims, Options.AuthenticationType);
                var identities = new List<ClaimsIdentity> { identity };
                var principal = new ClaimsPrincipal(identities);
                var ticket = new AuthenticationTicket(principal, Options.Scheme);

                return AuthenticateResult.Success(ticket);
            }

            return AuthenticateResult.Fail("Invalid API Key provided.");
        }

        protected override async Task HandleChallengeAsync(AuthenticationProperties properties)
        {
            Response.StatusCode = 401;
            Response.ContentType = ProblemDetailsContentType;
            var problemDetails = new UnauthorizedProblemDetails();

            await Response.WriteAsync(JsonSerializer.Serialize(problemDetails));
        }

        protected override async Task HandleForbiddenAsync(AuthenticationProperties properties)
        {
            Response.StatusCode = 403;
            Response.ContentType = ProblemDetailsContentType;
            var problemDetails = new ForbiddenProblemDetails();

            await Response.WriteAsync(JsonSerializer.Serialize(problemDetails));
        }
    }
}
