// This code is property of the GGAO // 


using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace techhubapigw.Models
{
    public class MetricList
    {
        public DateTime Timestamp { get; set; }

        public string Department { get; set; }

        public string Resource { get; set; }


        public long Count { get; set; }
    }
}
