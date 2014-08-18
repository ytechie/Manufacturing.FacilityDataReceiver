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
    }
}
