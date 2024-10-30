// This code is property of the GGAO // 


using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace techhubapigw.Configuration
{
    public class UhisConfiguration
    {
        public const string Section = "Uhis";

        public string Namespace { get; set; }

        public string PrefixNamespace { get; set; }

        public Dictionary<string,string> DefaultEntryPoints { get; set; }
    }
}
