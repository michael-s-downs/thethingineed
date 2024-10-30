// This code is property of the GGAO // 


using System;
using System.Linq;
using System.Threading.Tasks;

namespace techhubapigw.Database.DTOs
{
    public class Metric
    {
        public int Id { get; set; }
        public DateTime Timestamp { get; set; }

        public string Resource { get; set; }
        public long Count { get; set; }

        // One to many
        //public string ApiKeyReportId { get; set; }
        public string ApiKeyId { get; set; }
        public ApiKeyDTO Apikey { get; set; }
    }
}
