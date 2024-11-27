// This code is property of the GGAO // 


using Microsoft.AspNetCore.Authorization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using techhubapigw.Auth.Requirements;

namespace techhubapigw.Auth.Handlers
{
    // Docs: https://docs.microsoft.com/es-es/aspnet/core/security/authorization/policies?view=aspnetcore-3.1#use-a-handler-for-multiple-requirements
    public class OnlyThirdPartiesAuthorizationHandler : AuthorizationHandler<OnlyThirdPartiesRequirement>
    {
        protected override Task HandleRequirementAsync(AuthorizationHandlerContext context, OnlyThirdPartiesRequirement requirement)
        {
            if (context.User.IsInRole(Roles.ThirdParty))
            {
                context.Succeed(requirement);
            }

            return Task.CompletedTask;
        }
    }
}
