// This code is property of the GGAO // 


using Microsoft.AspNetCore.Authorization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using techhubapigw.Auth.Requirements;

namespace techhubapigw.Auth.Handlers
{
    public class OnlyTrainersAuthorizationHandler : AuthorizationHandler<OnlyTrainersRequirement>
    {
        protected override Task HandleRequirementAsync(AuthorizationHandlerContext context, OnlyTrainersRequirement requirement)
        {
            if (context.User.IsInRole(Roles.Trainer))
            {
                context.Succeed(requirement);
            }

            return Task.CompletedTask;
        }
    }
}
