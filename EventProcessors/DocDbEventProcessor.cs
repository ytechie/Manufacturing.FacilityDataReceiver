using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using log4net;
using Manufacturing.Framework.Datasource;
using Manufacturing.Framework.Dto;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Microsoft.Practices.ServiceLocation;
using Microsoft.ServiceBus.Messaging;
using WorkerRoleWithSBQueue1.Configuration;

namespace Manufacturing.FacilityDataProcessor.EventProcessors
{
    public class DocDbEventProcessor : IConsumerGroupEventProcessor
    {
        private static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private readonly CloudConfiguration _cloudConfig;
        private readonly IDatasourceRecordSerializer _datasourceRecordSerializer;

        private bool _shuttingDown;

        private const int MaxBatchSize = 5000;
        private int MaxBatchTimeMS = 10000;

        private static string CollectionSelfLink { get; set; }

        private const string DataTableName = "Raw";

        public DocDbEventProcessor()
        {
            //We have to get the config from the container unfortunately since this class
            //is being constructed by the event processor host
            _cloudConfig = ServiceLocator.Current.GetInstance<CloudConfiguration>();
            _datasourceRecordSerializer = ServiceLocator.Current.GetInstance<IDatasourceRecordSerializer>();
        }

        public string ConsumerGroupName
        {
            get { return "DocDb"; }
        }

        public bool RealTimeOnly
        {
            get { return false; }
        }

        public async Task OpenAsync(PartitionContext context)
        {
            await InitDocDbClient();
        }

        public async Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            var sw = new Stopwatch();
            var batch = new List<EventData>();

            sw.Start();
            foreach (var message in messages)
            {
                batch.Add(message);
                if (_shuttingDown || batch.Count >= MaxBatchSize || sw.ElapsedMilliseconds >= MaxBatchTimeMS)
                {
                    ProcessBatch(batch);
                    await context.CheckpointAsync();

                    batch.Clear();
                    sw.Reset();
                    sw.Start();
                }

                if (_shuttingDown)
                {
                    return;
                }
            }

            if (batch.Count > 0)
            {
                ProcessBatch(batch);
                await context.CheckpointAsync();
            }
        }

        public Task CloseAsync(PartitionContext context, CloseReason reason)
        {
            _shuttingDown = true;
            return null;
        }

        private void ProcessBatch(IList<EventData> messages)
        {
            var records = new List<DatasourceRecord>();

            foreach (var message in messages)
            {
                var body = message.GetBody<Stream>();
                var deserializedRecords = _datasourceRecordSerializer.Deserialize(body);
                records.AddRange(deserializedRecords);
            }

            InsertRecords(records);
        }

        private async Task InitDocDbClient()
        {
            using (var client = new DocumentClient(new Uri(_cloudConfig.DocDbUri), _cloudConfig.DocDbKey))
            {
                await client.OpenAsync();

                //This code is not as efficient as storing the selfLink in the config, but
                //I wanted to keep it easy to configure.

                var dynamicDb =
                    client.CreateDatabaseQuery("select * from root r where r.id = '" + _cloudConfig.DocDbDatabaseName +
                                               "'")
                        .AsEnumerable().FirstOrDefault();

                string dbSelfLink;
                if (dynamicDb == null)
                {
                    var db = new Database {Id = _cloudConfig.DocDbDatabaseName};
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
                    collection = new DocumentCollection {Id = DataTableName};
                    collection = await client.CreateDocumentCollectionAsync(dbSelfLink, collection);
                }

                //Caching the selfLink will help avoid querying the collections excessively
                CollectionSelfLink = collection.SelfLink;
            }
        }

        private void InsertRecords(IEnumerable<DatasourceRecord> recordBatch)
        {
            using (var client = new DocumentClient(new Uri(_cloudConfig.DocDbUri), _cloudConfig.DocDbKey))
            {
                var sw = new Stopwatch();
                sw.Start();
                var recs = 0;

                //Todo: Batch insert or transactional insert?
                foreach (var record in recordBatch)
                {
                    client.CreateDocumentAsync(CollectionSelfLink, record);
                    recs++;
                }
                sw.Stop();
                Log.DebugFormat("Inserted {0} records into Document DB in {1}ms", recs,
                    sw.ElapsedMilliseconds);
            }
        }
    }
}
