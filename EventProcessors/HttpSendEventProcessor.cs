using System.Collections.Generic;
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
using WorkerRoleWithSBQueue1.Configuration;

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

            var content = new ObjectContent<List<DatasourceRecord>>(records, new JsonMediaTypeFormatter());

            var postResult = await client.PostAsync("http://localhost:3184/api/data", content);
            if (postResult.StatusCode == HttpStatusCode.Created)
            {
                await context.CheckpointAsync();
            }
        }

        public Task CloseAsync(PartitionContext context, CloseReason reason)
        {
            return null;
        }

 
    }
}
