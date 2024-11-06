// This code is property of the GGAO // 


using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;

namespace techhubapigw.Database.DTOs
{
    public class Tenant
    {
        // Used to route, so only lowercase and ascii characters
        public string TenantId { get; set; }

        public string Name { get; set; }

        public Dictionary<string, string> EntryPoints { get; set; }

        public ICollection<ApiKeyDTO> ApiKeys { get; set; } = new Collection<ApiKeyDTO>();
    }
}
