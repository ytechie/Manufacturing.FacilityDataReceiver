using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Net.Http.Formatting;
using System.Reflection;
using System.Threading.Tasks;
using log4net;
using Manufacturing.Framework.Datasource;
using Manufacturing.Framework.Dto;
using Microsoft.Practices.ServiceLocation;
using Microsoft.ServiceBus.Messaging;

namespace Manufacturing.FacilityDataProcessor.EventProcessors
{
    public class HttpSendEventProcessor : IConsumerGroupEventProcessor
    {
        private static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private readonly IDatasourceRecordSerializer _datasourceRecordSerializer;

        private int MaxBatchTimeMS = 10000;

        public HttpSendEventProcessor()
        {
            //We have to get the config from the container unfortunately since this class
            //is being constructed by the event processor host
            _datasourceRecordSerializer = ServiceLocator.Current.GetInstance<IDatasourceRecordSerializer>();
        }

        public string ConsumerGroupName
        {
            get { return "HttpSend"; }
        }

        public bool RealTimeOnly
        {
            get { return true; }
        }

        public Task OpenAsync(PartitionContext context)
        {
            return Task.FromResult<object>(null);
        }

        public async Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            var client = new HttpClient();
            var records = new List<DatasourceRecord>();

            foreach (var message in messages)
            {
                var newRecords = _datasourceRecordSerializer.Deserialize(message.GetBodyStream());
                records.AddRange(newRecords);
            }

#if(DEBUG)
            const string postUrl = "http://localhost:3184/api/data";
#else
            const string postUrl = "http://mfxapi.azurewebsites.net/api/data";
#endif
            var content = new ObjectContent<List<DatasourceRecord>>(records, new JsonMediaTypeFormatter());

            var sw = new Stopwatch();
            sw.Start();
            var postResult = await client.PostAsync(postUrl, content);
            sw.Stop();
            Log.DebugFormat("Posted {0} messages to '{1}' in {2}ms", records.Count, postUrl, sw.ElapsedMilliseconds);

            if (postResult.StatusCode == HttpStatusCode.Created || postResult.StatusCode == HttpStatusCode.NoContent)
            {
                await context.CheckpointAsync();
            }
            else
            {
                Log.WarnFormat("Received status code '{0}' in HTTP POST", postResult.StatusCode);
            }
        }

        public Task CloseAsync(PartitionContext context, CloseReason reason)
        {
            return null;
        }

 
    }
}
