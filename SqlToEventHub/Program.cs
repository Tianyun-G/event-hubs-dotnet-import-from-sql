using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.Azure;
using Microsoft.WindowsAzure.ServiceRuntime;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using WorkerHost.Extensions;
using WorkerHost.SQL;

namespace WorkerHost
{
    public class WorkerHost : RoleEntryPoint
    {
        static void Main()
        {
            StartPushingUpdates("readerLocal");
        }

        public override void Run()
        {
            StartPushingUpdates("readerWorkerRole");
        }

        private static void StartPushingUpdates(string readeName)
        {
            const int SEND_GROUP_SIZE = 30;

            int sleepTimeMs;
            if (!int.TryParse(ConfigurationManager.AppSettings["SleepTimeMs"], out sleepTimeMs))
            {
                sleepTimeMs = 10000;
            }

            string sqlDatabaseConnectionString = ConfigurationManager.AppSettings["sqlDatabaseConnectionString"];
            string connectionString = ConfigurationManager.AppSettings["EventHubConnectionString"];
            string eventHubName = ConfigurationManager.AppSettings["EventHubName"];

            string dataTableName = ConfigurationManager.AppSettings["DataTableName"];
            string offsetKey = ConfigurationManager.AppSettings["OffsetKey"];

            string selectDataQueryTemplate =
                ReplaceDataTableName(
                    ReplaceOffsetKey(ReplaceReader(ConfigurationManager.AppSettings["DataQuery"], readeName),
                        offsetKey), dataTableName);

            string createOffsetTableQuery = ConfigurationManager.AppSettings["CreateOffsetTableQuery"];
            string selectOffsetQueryTemplate = ReplaceReader(ConfigurationManager.AppSettings["OffsetQuery"], readeName);
            string updateOffsetQueryTemplate = ReplaceReader(ConfigurationManager.AppSettings["UpdateOffsetQuery"], readeName);
            string insertOffsetQueryTemplate = ReplaceReader(ConfigurationManager.AppSettings["InsertOffsetQuery"], readeName);

            SqlTextQuery queryPerformer = new SqlTextQuery(sqlDatabaseConnectionString);
            queryPerformer.PerformQuery(createOffsetTableQuery);

            var producer = new EventHubProducerClient(connectionString, eventHubName);

            for (; ; )
            {
                try
                {
                    Dictionary<string, object> offsetQueryResult = queryPerformer.PerformQuery(selectOffsetQueryTemplate).FirstOrDefault();
                    string offsetString;
                    if (offsetQueryResult == null || offsetQueryResult.Count == 0)
                    {
                        offsetString = "-1";
                        queryPerformer.PerformQuery(ReplaceOffset(insertOffsetQueryTemplate, offsetString));
                    }
                    else
                    {
                        offsetString = offsetQueryResult.Values.First().ToString();
                    }

                    string selectDataQuery = ReplaceOffset(selectDataQueryTemplate, offsetString);

                    IEnumerable<Dictionary<string, object>> resultCollection =
                        queryPerformer.PerformQuery(selectDataQuery);

                    if (resultCollection.Any())
                    {
                        IEnumerable<Dictionary<string, object>> orderedByOffsetKey =
                            resultCollection.OrderBy(r => r[offsetKey]);

                        foreach (var resultGroup in orderedByOffsetKey.Split(SEND_GROUP_SIZE))
                        {
                            SendRowsToEventHub(producer, resultGroup).Wait();

                            string nextOffset = resultGroup.Max(r => r[offsetKey]).ToString();
                            queryPerformer.PerformQuery(ReplaceOffset(updateOffsetQueryTemplate, nextOffset));
                        }
                    }
                }
                catch (Exception)
                {
                    //TODO: log exception details
                }
                Thread.Sleep(sleepTimeMs);
            }
        }

        private static async Task SendRowsToEventHub(EventHubProducerClient producer, IEnumerable<object> rows)
        {
            var memoryStream = new MemoryStream();

            using (var sw = new StreamWriter(memoryStream, new UTF8Encoding(false), 1024, leaveOpen: true))
            {
                string serialized = JsonConvert.SerializeObject(rows);
                sw.Write(serialized);
                sw.Flush();
            }

            Debug.Assert(memoryStream.Position > 0, "memoryStream.Position > 0");

            memoryStream.Position = 0;
            BinaryData binaryData = BinaryData.FromStream(memoryStream);
            EventData eventData = new EventData(binaryData);

            try
            {
                EventDataBatch eventDataBatch = await producer.CreateBatchAsync();
                if (!eventDataBatch.TryAdd(eventData))
                {
                    throw new Exception($"The event could not be added.");
                }
                await producer.SendAsync(eventDataBatch);
            }
            finally
            {
                await producer.CloseAsync();
            }
        }

        private static string ReplaceDataTableName(string query, string tableName)
        {
            return query.Replace("{DataTableName}", tableName);
        }

        private static string ReplaceOffsetKey(string query, string offsetKey)
        {
            return query.Replace("{offsetKey}", offsetKey);
        }

        private static string ReplaceOffset(string query, string offsetValue)
        {
            return query.Replace("{offsetValue}", offsetValue);
        }

        private static string ReplaceReader(string query, string readerValue)
        {
            return query.Replace("{readerValue}", "\'" + readerValue + "\'");
        }
    }
}
