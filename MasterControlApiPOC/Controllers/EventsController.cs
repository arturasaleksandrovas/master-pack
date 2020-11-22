using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Mail;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Hangfire;
using MasterControlApiPOC.Models;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;

namespace MasterControlApiPOC.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class EventsController : ControllerBase
    {
        private readonly IBackgroundJobClient backgroundJobClient;

        public EventsController(IBackgroundJobClient backgroundJobClient)
        {
            this.backgroundJobClient = backgroundJobClient;
        }
        // GET api/values
        [HttpGet]
        public ActionResult<IEnumerable<string>> Get()
        {
            return new string[] { "value1", "value2" };
        }

        // GET api/values/5
        [HttpGet("{id}")]
        public ActionResult<string> Get(int id)
        {
            ReadFile();
            return "value";
        }

        // GET api/values/5
        [HttpGet("getstatuses")]
        public ActionResult<List<JobStatus>> GetStatuses()
        {
            List<JobStatus> jobsStatuses = new List<JobStatus>();

            using (SqlConnection connection = new SqlConnection("server=.;database=Safire;User Id=sa;Password=Strong7890;"))
            {
                string query = "SELECT JobId, Status FROM dbo.Jobstatuses ORDER BY Id DESC";
                SqlCommand command = new SqlCommand(query, connection);
                connection.Open();
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var js = new JobStatus { JobId = reader.GetInt32(0), Status = reader.GetString(1) };
                        jobsStatuses.Add(js);
                    }
                }
                connection.Close();
            }

            return jobsStatuses;
        }

        // GET api/values/5
        [HttpGet("gettriggers")]
        public ActionResult<List<EventTrigger>> GetTriggers()
        {
            List<EventTrigger> eventTriggers = new List<EventTrigger>();

            using (SqlConnection connection = new SqlConnection("server=.;database=Safire;User Id=sa;Password=Strong7890;"))
            {
                string query = "SELECT EventId, EventName, Params, EventTime FROM dbo.EventTriggers ORDER BY Id DESC";
                SqlCommand command = new SqlCommand(query, connection);
                connection.Open();
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var et = new EventTrigger { EventId = reader.GetInt32(0), EventName = reader.GetString(1), Params = reader.GetString(2), EventTime = reader.GetDateTime(3).ToString("yyyy-MM-dd HH:mm:ss.fff") };
                        eventTriggers.Add(et);
                    }
                }
                connection.Close();
            }

            return eventTriggers;
        }

        // POST api/values
        [HttpPost]
        public void Resolve([FromBody] EventTrigger eventTrigger)
        {
            var mtts = DateTimeOffset.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");

            using (SqlConnection connection = new SqlConnection("server=.;database=Safire;User Id=sa;Password=Strong7890;"))
            {
                string query = "INSERT INTO dbo.EventTriggers (EventId, EventName, Params, EventTime)" +
                    " output INSERTED.ID VALUES (@etei, @eten, @etp, @etet)";
                SqlCommand command = new SqlCommand(query, connection);
                command.Parameters.AddWithValue("@etei", eventTrigger.EventId);
                command.Parameters.AddWithValue("@eten", eventTrigger.EventName);
                command.Parameters.AddWithValue("@etp", eventTrigger.Params == null ? string.Empty : eventTrigger.Params);
                command.Parameters.AddWithValue("@etet", mtts);

                connection.Open();
                var newId = (int)command.ExecuteScalar();
                connection.Close();
            }

            if (eventTrigger.Type == "immediate")
            {
                Dictionary<int, string> jobs = new Dictionary<int, string>();

                using (SqlConnection connection = new SqlConnection("server=.;database=Safire;User Id=sa;Password=Strong7890;"))
                {
                    string query = "SELECT JobId, JobName FROM dbo.EventDependencies WHERE DependentEventId = " + eventTrigger.EventId;
                    SqlCommand command = new SqlCommand(query, connection);
                    connection.Open();
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                            jobs.Add(reader.GetInt32(0), reader.GetString(1));
                    }
                    connection.Close();
                }

                foreach (var job in jobs)
                {
                    backgroundJobClient.Enqueue(() => ResolveJob(job.Value));
                }
            }
        }

        // POST api/values
        [HttpPost("finish")]
        public void Finish([FromBody] JobStatus jobStatus)
        {
            using (SqlConnection connection = new SqlConnection("server=.;database=Safire;User Id=sa;Password=Strong7890;"))
            {
                string query = "INSERT INTO dbo.JobStatuses (JobId, Status)" +
                    " VALUES (@jsji, @jss)";
                SqlCommand command = new SqlCommand(query, connection);
                command.Parameters.AddWithValue("@jsji", jobStatus.JobId);
                command.Parameters.AddWithValue("@jss", jobStatus.Status);

                connection.Open();
                var newId = (int)command.ExecuteScalar();
                connection.Close();
            }
        }


        // POST api/values
        [HttpPost("startandprocess")]
        public void StartAndProcess([FromBody] JobStatus jobStatus)
        {
            using (SqlConnection connection = new SqlConnection("server=.;database=Safire;User Id=sa;Password=Strong7890;"))
            {
                string query = "INSERT INTO dbo.JobStatuses (JobId, Status)" +
                    " VALUES (@jsji, @jss)";
                SqlCommand command = new SqlCommand(query, connection);
                command.Parameters.AddWithValue("@jsji", jobStatus.JobId);
                command.Parameters.AddWithValue("@jss", jobStatus.Status);

                connection.Open();
                var newId = (int)command.ExecuteScalar();
                connection.Close();
            }
        }

        // PUT api/values/5
        [HttpPut("{id}")]
        public void Put(int id, [FromBody] string value)
        {
        }

        // DELETE api/values/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }

        [DisplayName("Job run with id #{0}")]
        public async Task ResolveJob(string name)
        {
            switch(name)
            {
                case "Balance_Trigger_Validation":
                    await BalanceTriggerValidation();
                    break;
                case "Balance_Trigger_Enrichment":
                    await BalanceTriggerEnrichment();
                    break;
                case "Balance_Trigger_Validation_Email":
                    await BalanceTriggerValidationEmail();
                    break;
                case "Balance_Trigger_ControlLine":
                    await BalanceTriggerControlLine();
                    break;
                default:
                    break;
            }
        }

        public void ReadFile()
        {
            var mtts = DateTimeOffset.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
            var query = new StringBuilder();

            string line;
            System.IO.StreamReader file =
                new System.IO.StreamReader("transactiondata.txt");
           

            while ((line = file.ReadLine()) != null)
            {
                var transaction = line.Split(',');

                query.Append("INSERT INTO dbo.Transactions (Account, Amount, Currency, MTTS) " +
                    "VALUES ('" + transaction[1] + "', '" + transaction[2] + "', '" + transaction[3] + "', '" + mtts + "'); ");

            }

            file.Close();

            using (SqlConnection connection = new SqlConnection("server=.;database=Safire;User Id=sa;Password=Strong7890;"))
            {
                SqlCommand command = new SqlCommand(query.ToString(), connection);
                connection.Open();
                command.ExecuteNonQuery();
                connection.Close();
            }
        }

        public async Task BalanceTriggerValidation()
        {
            var client = new HttpClient();

            var jobStatus0 = new JobStatus();
            jobStatus0.JobId = 1;
            jobStatus0.Status = "startandprocess";

            var json0 = JsonConvert.SerializeObject(jobStatus0);
            var data0 = new StringContent(json0, Encoding.UTF8, "application/json");

            await client.PostAsync("https://localhost:5001/api/events/startandprocess", data0);

            //Thread.Sleep(5000);

            Dictionary<int, Transaction> transactions = new Dictionary<int, Transaction>();

            using (SqlConnection connection = new SqlConnection("server=.;database=Safire;User Id=sa;Password=Strong7890;"))
            {
                string query = "SELECT Id, Account, Amount, Currency FROM dbo.Transactions WHERE MTTS = (SELECT TOP 1 MTTS FROM dbo.Transactions ORDER BY MTTS DESC)";
                SqlCommand command = new SqlCommand(query, connection);
                connection.Open();
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var ts = new Transaction { Account = reader.GetString(1), Amount = reader.GetDecimal(2), Currency = reader.GetString(3) };
                        transactions.Add(reader.GetInt32(0), ts);

                    }
                }
                connection.Close();
            }

            Dictionary<int, Transaction> validTransactions = new Dictionary<int, Transaction>();

            foreach (var transaction in transactions)
            {
               
                if (transaction.Value.Account != "3912345678")
                {
                    validTransactions.Add(transaction.Key, transaction.Value);
                }
            }

            var mtts = DateTimeOffset.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
            var query2 = new StringBuilder();

            foreach (var transaction in validTransactions) {

                query2.Append("INSERT INTO dbo.TransactionsValid (Account, Amount, Currency, MTTS) " +
                    "VALUES ('" + transaction.Value.Account + "', '" + transaction.Value.Amount + "', '" + transaction.Value.Currency + "', '" + mtts + "'); ");

            }

            using (SqlConnection connection = new SqlConnection("server=.;database=Safire;User Id=sa;Password=Strong7890;"))
            {
                SqlCommand command = new SqlCommand(query2.ToString(), connection);
                connection.Open();
                command.ExecuteNonQuery();
                connection.Close();
            }

            var jobStatus = new JobStatus();
            jobStatus.JobId = 1;
            jobStatus.Status = "finished";

            var json = JsonConvert.SerializeObject(jobStatus);
            var data = new StringContent(json, Encoding.UTF8, "application/json");

            await client.PostAsync("https://localhost:5001/api/events/finish", data);

            var eventTrigger = new EventTrigger();
            eventTrigger.EventName = "balancetriggervalidationsuccess";
            eventTrigger.Type = "immediate";
            eventTrigger.EventId = 11;

            json = JsonConvert.SerializeObject(eventTrigger);
            data = new StringContent(json, Encoding.UTF8, "application/json");

            await client.PostAsync("https://localhost:5001/api/events", data);
        }

        public async Task BalanceTriggerEnrichment()
        {
            var client = new HttpClient();

            var jobStatus0 = new JobStatus();
            jobStatus0.JobId = 2;
            jobStatus0.Status = "startandprocess";

            var json0 = JsonConvert.SerializeObject(jobStatus0);
            var data0 = new StringContent(json0, Encoding.UTF8, "application/json");

            await client.PostAsync("https://localhost:5001/api/events/startandprocess", data0);

            Thread.Sleep(5000);

            var jobStatus = new JobStatus();
            jobStatus.JobId = 2;
            jobStatus.Status = "finished";

            var json = JsonConvert.SerializeObject(jobStatus);
            var data = new StringContent(json, Encoding.UTF8, "application/json");

            await client.PostAsync("https://localhost:5001/api/events/finish", data);

            var eventTrigger = new EventTrigger();
            eventTrigger.EventName = "balancetriggerenrichmentsuccess";
            eventTrigger.Type = "immediate";
            eventTrigger.EventId = 12;

            json = JsonConvert.SerializeObject(eventTrigger);
            data = new StringContent(json, Encoding.UTF8, "application/json");

            await client.PostAsync("https://localhost:5001/api/events", data);
        }

        public async Task BalanceTriggerValidationEmail()
        {
            var client = new HttpClient();

            var jobStatus0 = new JobStatus();
            jobStatus0.JobId = 3;
            jobStatus0.Status = "startandprocess";

            var json0 = JsonConvert.SerializeObject(jobStatus0);
            var data0 = new StringContent(json0, Encoding.UTF8, "application/json");

            await client.PostAsync("https://localhost:5001/api/events/startandprocess", data0);

            Thread.Sleep(10000);

            //var smtpClient = new SmtpClient("smtp.gmail.com")
            //{
            //    Port = 587,
            //    Credentials = new NetworkCredential("midime@gmail.com", "nesakysiuniekam"),
            //    EnableSsl = true,
            //    Host = "smtp.gmail.com",
            //    DeliveryMethod = SmtpDeliveryMethod.Network,
            //    UseDefaultCredentials = false
            //};

            //smtpClient.Send("midime@gmail.com", "arturas.aleksandrov@gmail.com", "Test e-mail for valid transactions", "This is test e-mail for valid transactions");

            var jobStatus = new JobStatus();
            jobStatus.JobId = 3;
            jobStatus.Status = "finished";

            var json = JsonConvert.SerializeObject(jobStatus);
            var data = new StringContent(json, Encoding.UTF8, "application/json");

            await client.PostAsync("https://localhost:5001/api/events/finish", data);

            var eventTrigger = new EventTrigger();
            eventTrigger.EventName = "balancetriggervalidationemailsuccess";
            eventTrigger.Type = "immediate";
            eventTrigger.EventId = 13;

            json = JsonConvert.SerializeObject(eventTrigger);
            data = new StringContent(json, Encoding.UTF8, "application/json");

            await client.PostAsync("https://localhost:5001/api/events", data);
        }

        public async Task BalanceTriggerControlLine()
        {
            var client = new HttpClient();

            var jobStatus0 = new JobStatus();
            jobStatus0.JobId = 4;
            jobStatus0.Status = "startandprocess";

            var json0 = JsonConvert.SerializeObject(jobStatus0);
            var data0 = new StringContent(json0, Encoding.UTF8, "application/json");

            await client.PostAsync("https://localhost:5001/api/events/startandprocess", data0);

            Dictionary<int, Transaction> validTransactions = new Dictionary<int, Transaction>();

            using (SqlConnection connection = new SqlConnection("server=.;database=Safire;User Id=sa;Password=Strong7890;"))
            {
                string query = "SELECT Id, Account, Amount, Currency FROM dbo.TransactionsValid WHERE MTTS = (SELECT TOP 1 MTTS FROM dbo.TransactionsValid ORDER BY MTTS DESC)";
                SqlCommand command = new SqlCommand(query, connection);
                connection.Open();
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var ts = new Transaction { Account = reader.GetString(1), Amount = reader.GetDecimal(2), Currency = reader.GetString(3) };
                        validTransactions.Add(reader.GetInt32(0), ts);

                    }
                }
                connection.Close();
            }
            List<ControlLine> controlLines = new List<ControlLine>();
            foreach (var transaction in validTransactions)
            {
                if (!controlLines.Any(x => x.Account == transaction.Value.Account))
                {
                    controlLines.Add(new ControlLine
                    {
                        Account = transaction.Value.Account,
                        Currency = transaction.Value.Currency,
                        TransactionCount = 0
                    });
                }

                var newValue = controlLines.First(x => x.Account == transaction.Value.Account);
                controlLines.RemoveAll(x => x.Account == transaction.Value.Account);

                newValue.Amount += transaction.Value.Amount;
                newValue.TransactionCount++;

                controlLines.Add(newValue);
            }

            using (StreamWriter writetext = new StreamWriter("controlfile.txt"))
            {
                foreach (var controlLine in controlLines)
                {

                    writetext.WriteLine(controlLine.Account + "," + controlLine.Amount + "," + controlLine.TransactionCount);
                }

            }

            Thread.Sleep(5000);

            var jobStatus = new JobStatus();
            jobStatus.JobId = 4;
            jobStatus.Status = "finished";

            var json = JsonConvert.SerializeObject(jobStatus);
            var data = new StringContent(json, Encoding.UTF8, "application/json");

            await client.PostAsync("https://localhost:5001/api/events/finish", data);

            var eventTrigger = new EventTrigger();
            eventTrigger.Params = JsonConvert.SerializeObject(new List<string> { "success", "controlfile.txt" });
            eventTrigger.EventName = "balancetriggercontrollinesuccess";
            eventTrigger.Type = "immediate";
            eventTrigger.EventId = 14;

            json = JsonConvert.SerializeObject(eventTrigger);
            data = new StringContent(json, Encoding.UTF8, "application/json");

            await client.PostAsync("https://localhost:5001/api/events", data);
        }
    }
}
