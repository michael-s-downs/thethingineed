// This code is property of the GGAO // 


using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using techhubapigw.Database.DTOs;

namespace techhubapigw.Models
{
    public class ApiKeyServiceState
    {
        public ConcurrentDictionary<string, ApiKeyDTO> apiKeys { get; } = new ConcurrentDictionary<string, ApiKeyDTO>();
        public ConcurrentDictionary<string, DateTime> apiKeysLastAccess { get; } = new ConcurrentDictionary<string, DateTime>();

        // ReportId -> Key
        public ConcurrentDictionary<string, string> apiKeysByUsage { get; } = new ConcurrentDictionary<string, string>();


        public ConcurrentDictionary<string, UsageLimit> usages { get; } = new ConcurrentDictionary<string, UsageLimit>();
    }
}
