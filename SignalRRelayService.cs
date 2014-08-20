using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using Bootstrap.Extensions.StartupTasks;
using log4net;
using Manufacturing.Framework.Datasource;
using Manufacturing.Framework.Dto;
using Microsoft.FluentMessaging;
using WorkerRoleWithSBQueue1.Clients;
using WorkerRoleWithSBQueue1.Configuration;

namespace Manufacturing.FacilityDataProcessor
{
    /// <summary>
    ///     Receives messages from the Service Bus and relays them to a SignalR hub
    /// </summary>
    public class SignalRRelayService : IStartupTask
    {
        #region Fields

        private static readonly ILog _log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private readonly CloudConfiguration _config;

        private readonly IDatasourceRecordSerializer _serializer;

        #endregion

        #region Constructors

        public SignalRRelayService(CloudConfiguration configuration, IDatasourceRecordSerializer serializer)
        {
            _config = configuration;
            _serializer = serializer;
        }

        #endregion

        #region IStartupTask Members

        public void Reset()
        {
            _log.Debug("Reset called but not supported.");
        }

        public void Run()
        {
            var batchProcess = new Action<IEnumerable<DatasourceRecord>>(recordBatch =>
            {
                var recordsSent = 0;
                var sw = new Stopwatch();
                sw.Start();
                try
                {
                    using(
                        var client = new SignalRRelayClient<DatasourceRecord>(_config.StreamingDataEndpointUrl,
                            _config.StreamingDataEndpointHubId))
                    {
                        foreach(var record in recordBatch)
                        {
                            client.Send(_config.StreamingDataEndpointMethodId, record);
                            recordsSent++;
                        }
                    }

                    sw.Stop();
                    _log.DebugFormat("Sent {0} records to SignalR endpoint in {1}ms", recordsSent,
                        sw.ElapsedMilliseconds);
                }
                catch(Exception ex)
                {
                    sw.Stop();
                    _log.Warn(
                        string.Format("Failed sending records to SignalR; sent {0} records in {1}ms before failing.",
                            recordsSent, sw.ElapsedMilliseconds), ex);
                }
            });

            QueueFramework.FromTopicSubscription(_config.ReceiveQueue.GetConnectionString(),
                _config.ReceiveQueue.QueueName, _config.ReceiveQueue.SubscriptionName)
                .WithMaxConcurrency(10)
                .OutputToRaw(_serializer as DatasourceRecordSerializer, batchProcess);
        }

        #endregion
    }
}