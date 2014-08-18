using System.Collections.Generic;
using System.IO;
using System.Reflection;
using log4net;
using Manufacturing.Framework.Datasource;
using Microsoft.FluentMessaging;
using Microsoft.ServiceBus.Messaging;

namespace WorkerRoleWithSBQueue1
{
    public class ReceiveQueuePassthrough : IQueueProcessor
    {
        private static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);



        public ReceiveQueuePassthrough()
        {

        }

        public void ProcessMessages(IEnumerable<BrokeredMessage> messages, IMessageSink output)
        {
            long approxMessageSize = 0;
            var outMessages = new List<BrokeredMessage>();

            foreach (var message in messages)
            {
                var stream = message.GetBody<Stream>();
                outMessages.Add(message.Clone());
            }

            Log.DebugFormat("Created {0} messages through a split", outMessages.Count);

            const double maxBatchSize = 256 * 1024;
            var batchSize = maxBatchSize / approxMessageSize * .8; //Factor in a bit of overhead for safety

            output.SendMessages(outMessages, (int)batchSize);
        }
    }
}
