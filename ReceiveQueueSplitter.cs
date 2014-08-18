//using System.Collections.Generic;
//using System.IO;
//using System.Reflection;
//using Microsoft.FluentMessaging;
//using log4net;
//using Manufacturing.Framework.Datasource;
//using Microsoft.ServiceBus.Messaging;

//namespace WorkerRoleWithSBQueue1
//{
//    public class ReceiveQueueSplitter : IQueueProcessor
//    {
//        private static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

//        private readonly IDatasourceRecordSerializer _recordSerializer;

//        public ReceiveQueueSplitter(IDatasourceRecordSerializer recordSerializer)
//        {
//            _recordSerializer = recordSerializer;
//        }

//        public void ProcessMessages(IEnumerable<BrokeredMessage> messages, IMessageSink output)
//        {
//            long approxMessageSize = 0;
//            var outMessages = new List<BrokeredMessage>();

//            foreach (var message in messages)
//            {
//                var stream = message.GetBody<Stream>();
//                var records = _recordSerializer.Deserialize(stream);

//                foreach (var record in records)
//                {
//                    //Can't wrap this in a using because that would dispose it
//                    var ms = new MemoryStream(); 
//                    _recordSerializer.Serialize(ms, new List<DatasourceRecord> {record});

//                    if (approxMessageSize == 0)
//                    {
//                        approxMessageSize = ms.Length + 156; //1635;
//                    }

//                    var newMessage = new BrokeredMessage(ms, true);
//                    outMessages.Add(newMessage);
//                }
//                Log.DebugFormat("Created {0} messages through a split", outMessages.Count);
//            }

//            const double maxBatchSize = 256 * 1024;
//            var batchSize = maxBatchSize / approxMessageSize * .8; //Factor in a bit of overhead for safety

//            output.SendMessages(outMessages, (int)batchSize);
//        }
//    }
//}
