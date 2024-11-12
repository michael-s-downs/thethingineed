// This code is property of the GGAO // 


using Microsoft.Extensions.Configuration;
using System.Collections.Generic; 

namespace techhubapigw.Cors
{
    public class CorsSettings
    {
        public List<string> AllowedOrigins { get; set; }
        public List<string> AllowedMethod { get; set; }
        public List<string> AllowedHeader { get; set; }
        public bool AllowCredentials { get; set; }
        public bool AllowAnyOrigin  { get; set; }
        public string MaxAge { get; set; }
    }
}
