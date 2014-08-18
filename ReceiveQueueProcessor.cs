//using System.Reflection;
//using Bootstrap.Extensions.StartupTasks;
//using Microsoft.FluentMessaging;
//using log4net;
//using WorkerRoleWithSBQueue1.Configuration;

//namespace WorkerRoleWithSBQueue1
//{
//    public class ReceiveQueueProcessor : IStartupTask
//    {
//        private static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

//        private readonly CloudConfiguration _config;

//        public ReceiveQueueProcessor(CloudConfiguration configuration)
//        {
//            _config = configuration;
//        }

//        public void Run()
//        {
//            QueueFramework
//                .FromTopicSubscription(_config.ReceiveQueue.GetConnectionString(), )
//                .WithMaxConcurrency(1)
//                .ProcessWith<ReceiveQueueSplitter>()
//                .OutputToTopic(_config.DatasourceRecordsTopic.GetConnectionString(), _config.DatasourceRecordsTopic.QueueName);
                
//        }

//        public void Reset()
//        {
//        }
//    }
//}
