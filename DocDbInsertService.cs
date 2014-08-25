using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Bootstrap.Extensions.StartupTasks;
using log4net;
using Manufacturing.Framework.Datasource;
using Manufacturing.Framework.Dto;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Microsoft.FluentMessaging;
using WorkerRoleWithSBQueue1.Configuration;

namespace Manufacturing.FacilityDataProcessor
{
    public class DocDbInsertService : IStartupTask
    {
        private static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private readonly CloudConfiguration _config;
        private readonly IDatasourceRecordSerializer _serializer;

        private static string CollectionSelfLink { get; set; }

        private const string DataTableName = "Raw";

        public DocDbInsertService(CloudConfiguration configuration, IDatasourceRecordSerializer serializer)
        {
            _config = configuration;
            _serializer = serializer;
        }

        public async void Run()
        {
            await InitDocDbClient();

            //ReadSample();

            var batchProcess = new Action<IEnumerable<DatasourceRecord>>(recordBatch =>
            {
                using (var client = new DocumentClient(new Uri(_config.DocDbUri), _config.DocDbKey))
                {
                    var tasks =
                        recordBatch.Select(record => client.CreateDocumentAsync(CollectionSelfLink, record))
                            .Cast<Task>()
                            .ToList();
                    var taskArr = tasks.ToArray();
                    Task.WaitAll(tasks.ToArray());
                    Log.DebugFormat("{0} records inserted into DocDB", taskArr.Length);
                }
            });

            QueueFramework
                .FromTopicSubscription(_config.ReceiveQueue.GetConnectionString(), _config.ReceiveQueue.QueueName,
                    _config.ReceiveQueue.SubscriptionName)
                    .WithMaxConcurrency(10)
                .OutputToRaw(_serializer as DatasourceRecordSerializer, batchProcess);
        }

        private async Task InitDocDbClient()
        {
            using (var client = new DocumentClient(new Uri(_config.DocDbUri), _config.DocDbKey))
            {
                await client.OpenAsync();

                //This code is not as efficient as storing the selfLink in the config, but
                //I wanted to keep it easy to configure.

                var dynamicDb = client.CreateDatabaseQuery("select * from root r where r.id = '" + _config.DocDbDatabaseName + "'")
                    .AsEnumerable().FirstOrDefault();

                string dbSelfLink;
                if (dynamicDb == null)
                {
                    var db = new Database { Id = _config.DocDbDatabaseName };
                    db = await client.CreateDatabaseAsync(db);
                    dbSelfLink = db.SelfLink;
                }
                else
                {
                    dbSelfLink = dynamicDb._self;
                }

                var collectionFeed = await client.ReadDocumentCollectionFeedAsync(dbSelfLink);
                var collection = collectionFeed.SingleOrDefault(x => x.Id == DataTableName);
                if (collection == null)
                {
                    collection = new DocumentCollection { Id = DataTableName };
                    collection = await client.CreateDocumentCollectionAsync(dbSelfLink, collection);
                }

                //Caching the selfLink will help avoid querying the collections excessively
                CollectionSelfLink = collection.SelfLink;
            }
        }

        //This is here simply as a reference to read the recorded data
        private void ReadSample()
        {
            using (var client = new DocumentClient(new Uri(_config.DocDbUri), _config.DocDbKey))
            {
                var query = client.CreateDocumentQuery(CollectionSelfLink, "select * from " + DataTableName);
                var results = query.AsEnumerable().ToList(); //Don't run this when the table is large!
            }
        }

        public void Reset()
        {
        }
    }
}
