// This code is property of the GGAO // 


using Microsoft.Extensions.Configuration;
using System.Collections.Generic; 

namespace techhubapigw.Cors
{
    public class CorsSettings
    {
        public List<string> AllowedOrigins { get; set; }
        public bool AllowAnyMethod { get; set; }
        public bool AllowAnyHeader { get; set; }
        public bool AllowCredentials { get; set; }
    }
}
