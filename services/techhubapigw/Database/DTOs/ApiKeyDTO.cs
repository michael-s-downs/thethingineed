// This code is property of the GGAO // 


using System;
using System.IO;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text.Json.Serialization;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;

namespace techhubapigw.Database.DTOs
{
    public class ApiKeyDTO
    {
        // Used as Primary key
        public string Key { get; set; }
        public DateTime Created { get; set; }
        public bool Enabled { get; set; }

        // Used as index to report ussage
        public string ReportId { get; set; }
        public string Department { get; set; }
        public string TenantId { get; set; }
        [JsonIgnore]
        public Tenant Tenant { get; set; }


        //public IReadOnlyCollection<string> Roles { get; set; }
        public List<string> Roles { get; set; }

        // Limits and metrics to query
        public ICollection<UsageLimit> Limits { get; set; } = new Collection<UsageLimit>();

        [JsonIgnore]
        public ICollection<Metric> Metrics { get; set; } = new Collection<Metric>();
    }
}
