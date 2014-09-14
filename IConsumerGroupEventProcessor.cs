using Microsoft.ServiceBus.Messaging;

namespace Manufacturing.FacilityDataProcessor
{
    public interface IConsumerGroupEventProcessor : IEventProcessor
    {
        string ConsumerGroupName { get; }
    }
}
