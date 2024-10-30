﻿// This code is property of the GGAO // 


using Microsoft.AspNetCore.Authentication;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace techhubapigw.Auth
{
    public class ApiKeyAuthenticationOptions : AuthenticationSchemeOptions
    {
        public const string DefaultScheme = "API-Key";
        public string Scheme => DefaultScheme;
        public string AuthenticationType = DefaultScheme;
    }

}
