using System;
using Bootstrap.Extensions.StartupTasks;
using Manufacturing.FacilityDataProcessor.EventProcessors;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using WorkerRoleWithSBQueue1.Configuration;

namespace Manufacturing.FacilityDataProcessor
{
    public class EventHubProcessor : IStartupTask
    {
        private EventProcessorHost _eventProcessorHost;
        private readonly CloudConfiguration _config;

        public EventHubProcessor(CloudConfiguration configuration)
        {
            _config = configuration;
        }

        public void Run()
        {
            var eventHubClient = EventHubClient.CreateFromConnectionString(_config.EventHubConnectionString, _config.EventHubRecieverPath);
            string consumerGroup;
            if (string.IsNullOrEmpty(_config.EventHubConsumerGroup))
            {
                consumerGroup = eventHubClient.GetDefaultConsumerGroup().GroupName;
            }
            else
            {
                consumerGroup = _config.EventHubConsumerGroup;

                var ns = NamespaceManager.CreateFromConnectionString(_config.EventHubConnectionString);
                ns.CreateConsumerGroupIfNotExistsAsync(_config.EventHubRecieverPath, consumerGroup);
            }

            _eventProcessorHost = new EventProcessorHost(Environment.MachineName, _config.EventHubRecieverPath,
                consumerGroup, _config.EventHubConnectionString, _config.EventHubStorageConnectionString);

            _eventProcessorHost.RegisterEventProcessorAsync<SqlDatabaseEventProcessor>().Wait();
        }

        public void Reset()
        {
            _eventProcessorHost.UnregisterEventProcessorAsync();
        }
    }
}
