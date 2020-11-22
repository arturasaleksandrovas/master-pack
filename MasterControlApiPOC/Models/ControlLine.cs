using System;
namespace MasterControlApiPOC.Models
{
    public class ControlLine
    {
        public string Account { get; set; }

        public int TransactionCount { get; set; }

        public decimal Amount { get; set; }

        public string Currency { get; set; }
    }
}
