// This code is property of the GGAO // 


using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace techhubapigw.Models
{
    public class TenantList
    {
        public string Id { get; set; }
        public string Name { get; set; }

        public Dictionary<string, string> EntryPoints { get; set; }

        public IEnumerable<ApiKeyList> ApiKeys { get; set; }
    }

    public class ApiKeyList
    {
        public string Key { get; set; }
        public string Department { get; set; }

        public IEnumerable<string> Roles { get; set; }
    }
}
