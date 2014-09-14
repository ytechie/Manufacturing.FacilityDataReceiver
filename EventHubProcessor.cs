using System;
using System.Collections.Generic;
using System.Threading.Tasks;
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
        private readonly IEnumerable<IConsumerGroupEventProcessor> _eventProcessors;

        public EventHubProcessor(CloudConfiguration configuration, IEnumerable<IConsumerGroupEventProcessor> eventProcessors)
        {
            _config = configuration;
            _eventProcessors = eventProcessors;
        }

        public void Run()
        {
            var processorTasks = new List<Task>();
            foreach (var eventProcessor in _eventProcessors)
            {
                var consumerGroupName = CreateConsumerGroupIfNeeded(eventProcessor.ConsumerGroupName);
                processorTasks.Add(StartEventProcessor(eventProcessor, consumerGroupName));
            }

            Task.WaitAll(processorTasks.ToArray());
        }

        private string CreateConsumerGroupIfNeeded(string consumerGroupName)
        {
            var eventHubClient = EventHubClient.CreateFromConnectionString(_config.EventHubConnectionString,
                _config.EventHubRecieverPath);
            if (string.IsNullOrEmpty(consumerGroupName))
            {
                return eventHubClient.GetDefaultConsumerGroup().GroupName;
            }

            var ns = NamespaceManager.CreateFromConnectionString(_config.EventHubConnectionString);
            ns.CreateConsumerGroupIfNotExistsAsync(_config.EventHubRecieverPath, consumerGroupName);

            return consumerGroupName;
        }

        private Task StartEventProcessor(IEventProcessor eventProcessor, string consumerGroupName)
        {
            _eventProcessorHost = new EventProcessorHost(Environment.MachineName, _config.EventHubRecieverPath,
                consumerGroupName, _config.EventHubConnectionString, _config.EventHubStorageConnectionString);

            var type = eventProcessor.GetType();
            return _eventProcessorHost.RegisterEventProcessorFactoryAsync(new EventProcessorFactory(type));
        }

        public void Reset()
        {
            _eventProcessorHost.UnregisterEventProcessorAsync();
        }
    }
}
