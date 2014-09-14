using Manufacturing.Framework.Configuration;

namespace WorkerRoleWithSBQueue1.Configuration
{
    public class CloudConfiguration
    {
        public ServiceBusTopicSubscriptionInformation ReceiveQueue { get; set; }

        public StorageConfiguration DataStorageConfiguration { get; set; }

        public int ReceiveQueueConnectionLimit { get; set; }

        public string SqlDatabaseConnectionString { get; set; }

        public string StreamingDataEndpointUrl { get; set; }

        public string StreamingDataEndpointHubId { get; set; }

        public string StreamingDataEndpointMethodId { get; set; }

        public string DocDbUri { get; set; }
        public string DocDbKey { get; set; }
        public string DocDbDatabaseName { get; set; }

        public string EventHubConnectionString { get; set; }
        public string EventHubRecieverPath { get; set; }
        public string EventHubConsumerGroup { get; set; }
        public string EventHubStorageConnectionString { get; set; }

        public string OrleansUrl { get; set; }
    }
}
