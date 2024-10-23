// This code is property of the GGAO // 


using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text.Json.Serialization;
using System.Linq;
using System.Threading.Tasks;

namespace techhubapigw.Database.DTOs
{
    public class UsageLimit
    {
        [JsonIgnore]
        public int Id { get; set; }
        public string Resource { get; set; }
        public int Current { get; set; }
        public int Limit { get; set; }

        // Reference
        [JsonIgnore]
        public string ApiKeyId { get; set; }
        [JsonIgnore]
        public ApiKeyDTO ApiKey { get; set; }
    }
}
