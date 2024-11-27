// This code is property of the GGAO // 


using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using techhubapigw.Services;

namespace techhubapigw.Middlewares
{
    public static class LicenseMiddlewareExtensions
    {
        public static IApplicationBuilder UseLicense(this IApplicationBuilder builder) => builder.UseMiddleware<LicenseMiddleware>();
    }

    // https://docs.microsoft.com/es-es/aspnet/core/fundamentals/middleware/write?view=aspnetcore-3.1
    public class LicenseMiddleware
    {

        private readonly RequestDelegate _next;

        public LicenseMiddleware(RequestDelegate next)
        {
            _next = next;
        }

        public async Task Invoke(HttpContext httpContext, ILicenseService licenseService) //, IMyScopedService svc)
        {
            // TODO: check if user has access (no blocking)

            
            await _next(httpContext);
        }
    }
}
