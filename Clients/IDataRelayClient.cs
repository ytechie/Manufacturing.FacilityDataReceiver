using System;
using System.Collections.Generic;

namespace WorkerRoleWithSBQueue1.Clients
{
    public interface IDataRelayClient<in T> : IDisposable
    {
        #region public

        void Send(string method, T message);

        void Send(string method, IEnumerable<T> messages);

        #endregion
    }
}