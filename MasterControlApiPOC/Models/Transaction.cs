using System;
namespace MasterControlApiPOC.Models
{
    public class Transaction
    {
        public string Account { get; set; }

        public decimal Amount { get; set; }

        public string Currency { get; set; }
    }
}
