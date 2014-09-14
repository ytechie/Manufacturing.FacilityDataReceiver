using System;
using Microsoft.Practices.ServiceLocation;
using Microsoft.ServiceBus.Messaging;

namespace Manufacturing.FacilityDataProcessor
{
    public class EventProcessorFactory : IEventProcessorFactory
    {
        private readonly Type _type;

        public EventProcessorFactory(Type type)
        {
            _type = type;
        }

        public IEventProcessor CreateEventProcessor(PartitionContext context)
        {
            return ServiceLocator.Current.GetInstance(_type) as IEventProcessor;
        }
    }
}
