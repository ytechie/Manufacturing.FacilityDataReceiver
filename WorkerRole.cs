using System.Reflection;
using System.Threading;
using Bootstrap;
using Bootstrap.Locator;
using Bootstrap.StructureMap;
using log4net;
using Manufacturing.DataCollector;
using Manufacturing.DataPusher;
using Manufacturing.Framework.Configuration;
using Manufacturing.Framework.Logging;
using Microsoft.Practices.ServiceLocation;
using Microsoft.WindowsAzure.ServiceRuntime;

namespace Manufacturing.FacilityDataProcessor
{
    public class WorkerRole : RoleEntryPoint
    {
        #region Fields

        private static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        // QueueClient is thread-safe. Recommended that you cache 
        // rather than recreating it on every request
        private readonly ManualResetEvent _completedEvent = new ManualResetEvent(false);

        #endregion

        #region public

        public override bool OnStart()
        {
            LoggingUtils.InitializeLogging();

            Bootstrapper.With.StructureMap()
                .And.ServiceLocator()
                .LookForTypesIn.ReferencedAssemblies()
                .Including.Assembly(Assembly.GetAssembly(typeof(FrameworkContainer)))
                .Including.Assembly(Assembly.GetAssembly(typeof(DataCollectorContainer)))
                .Including.Assembly(Assembly.GetAssembly(typeof(DataPusherContainer)))
                .Including.Assembly(Assembly.GetAssembly(typeof(WorkerRole)))
                .Start();

            var container = ServiceLocator.Current;

            var eventHubProcessor = container.GetInstance<EventHubProcessor>();
            eventHubProcessor.Run();

            return base.OnStart();
        }

        public override void OnStop()
        {
            // Close the connection to Service Bus Queue
            _completedEvent.Set();
            base.OnStop();
        }

        public override void Run()
        {
            _completedEvent.WaitOne();
        }

        #endregion
    }
}