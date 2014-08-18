using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography;
using Bootstrap.Extensions.StartupTasks;
using log4net;
using Manufacturing.Framework.Datasource;
using Manufacturing.Framework.Dto;
using Microsoft.FluentMessaging;
using WorkerRoleWithSBQueue1.Configuration;

namespace WorkerRoleWithSBQueue1
{
    public class SqlDatabaseInsertService : IStartupTask
    {
        private static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private readonly CloudConfiguration _config;
        private readonly IDatasourceRecordSerializer _serializer;

        public SqlDatabaseInsertService(CloudConfiguration configuration, IDatasourceRecordSerializer serializer)
        {
            _config = configuration;
            _serializer = serializer;
        }

        public void Run()
        {
            var batchProcess = new Action<IEnumerable<DatasourceRecord>>(recordBatch =>
            {
                var dt = new DataTable();
                dt.Columns.Add("Id", typeof(int));
                dt.Columns.Add("DatasourceId", typeof(int));
                dt.Columns.Add("Timestamp", typeof(DateTime));
                dt.Columns.Add("IntervalSeconds", typeof(int));
                dt.Columns.Add("Value", typeof(byte[]));
                dt.Columns.Add("EncodedDataType", typeof(int));

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
                    new SqlConnection(_config.SqlDatabaseConnectionString)
                    )
                {
                    var sw = new Stopwatch();
                    sw.Start();
                    connection.Open();
                    using (var bulk = new SqlBulkCopy(connection) { DestinationTableName = "dbo.Raw" })
                    {
                        bulk.WriteToServer(dt);
                    }
                    sw.Stop();
                    Log.DebugFormat("Inserted {0} records into SQL Azure Database in {1}ms", dt.Rows.Count,
                        sw.ElapsedMilliseconds);
                }
            });

            QueueFramework
                .FromTopicSubscription(_config.ReceiveQueue.GetConnectionString(), _config.ReceiveQueue.QueueName,
                    _config.ReceiveQueue.SubscriptionName)
                    .WithMaxConcurrency(10)
                .OutputToRaw(_serializer as DatasourceRecordSerializer, batchProcess);
        }

        public void Reset()
        {
        }
    }
}
