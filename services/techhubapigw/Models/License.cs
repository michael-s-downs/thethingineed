// This code is property of the GGAO // 


using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace techhubapigw.Models
{
    public class License
    {
        public int Id { get; set; }

        // Keys are formatted like: {department}/{resourceType}
        // Example: contability/dni
        // Example: insurance/issue-part
        ConcurrentDictionary<string, int> Counters { get; set; }
        Dictionary<string, int> Limits { get; set; }
    }
}
