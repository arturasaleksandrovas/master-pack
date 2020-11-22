using System;
namespace MasterControlApiPOC.Models
{
    public class EventTrigger
    {
        public int EventId { get; set; }

        public string Type { get; set; }

        public string EventName { get; set; }

        public string Params { get; set; }

        public string EventTime { get; set; }
    }
}
