using Bootstrap.StructureMap;
using StructureMap;
using StructureMap.Graph;
using StructureMap.Pipeline;

namespace Manufacturing.FacilityDataProcessor
{
    public class FaclityDataProcessorContainer : IStructureMapRegistration
    {
        public void Register(IContainer container)
        {
            container.Configure(x => x.Scan(y =>
            {
                y.TheCallingAssembly();
                y.AddAllTypesOf<IConsumerGroupEventProcessor>();
                y.SingleImplementationsOfInterface().OnAddedPluginTypes(z => z.LifecycleIs(new TransientLifecycle()));
                y.ExcludeType<EventProcessorFactory>();
            }));
        }
    }
}
