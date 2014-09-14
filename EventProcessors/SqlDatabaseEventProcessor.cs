using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using log4net;
using Manufacturing.Framework.Datasource;
using Manufacturing.Framework.Dto;
using Microsoft.Practices.ServiceLocation;
using Microsoft.ServiceBus.Messaging;
using WorkerRoleWithSBQueue1.Configuration;

namespace Manufacturing.FacilityDataProcessor.EventProcessors
{
    public class SqlDatabaseEventProcessor : IEventProcessor
    {
        private static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private readonly CloudConfiguration _cloudConfig;
        private readonly IDatasourceRecordSerializer _datasourceRecordSerializer;

        private bool _shuttingDown;

        private const int MaxBatchSize = 5000;
        private int MaxBatchTimeMS = 10000;

        public SqlDatabaseEventProcessor()
        {
            //We have to get the config from the container unfortunately since this class
            //is being constructed by the event processor host
            _cloudConfig = ServiceLocator.Current.GetInstance<CloudConfiguration>();
            _datasourceRecordSerializer = ServiceLocator.Current.GetInstance<IDatasourceRecordSerializer>();
        }

        public Task OpenAsync(PartitionContext context)
        {
            return Task.FromResult<object>(null);
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

        private void InsertRecords(IEnumerable<DatasourceRecord> recordBatch)
        {
            var dt = new DataTable();
            dt.Columns.Add("Id", typeof (int));
            dt.Columns.Add("DatasourceId", typeof (int));
            dt.Columns.Add("Timestamp", typeof (DateTime));
            dt.Columns.Add("IntervalSeconds", typeof (int));
            dt.Columns.Add("Value", typeof (byte[]));
            dt.Columns.Add("EncodedDataType", typeof (int));

            foreach (var record in recordBatch)
            {
                var dr = dt.NewRow();
                dr["DatasourceId"] = record.DatasourceId;
                dr["Timestamp"] = record.Timestamp;
                dr["IntervalSeconds"] = record.IntervalSeconds;
                dr["Value"] = record.Value;
                dr["EncodedDataType"] = record.EncodedDataType;

                dt.Rows.Add(dr);
            }

            using (var connection =
                new SqlConnection(_cloudConfig.SqlDatabaseConnectionString)
                )
            {
                var sw = new Stopwatch();
                sw.Start();
                connection.Open();
                using (var bulk = new SqlBulkCopy(connection) {DestinationTableName = "dbo.Raw"})
                {
                    bulk.WriteToServer(dt);
                }
                sw.Stop();
                Log.DebugFormat("Inserted {0} records into SQL Azure Database in {1}ms", dt.Rows.Count,
                    sw.ElapsedMilliseconds);
            }
        }
    }
}
