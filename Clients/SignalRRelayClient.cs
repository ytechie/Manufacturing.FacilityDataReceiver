using System.Collections.Generic;
using Microsoft.AspNet.SignalR.Client;

namespace WorkerRoleWithSBQueue1.Clients
{
    public class SignalRRelayClient<T> : IDataRelayClient<T>
    {
        #region Fields

        private readonly HubConnection _hubConnection;

        private readonly IHubProxy _proxy;

        #endregion

        #region Constructors

        public SignalRRelayClient(string endpointUrl, string hubId)
        {
            _hubConnection = new HubConnection(endpointUrl);
            _proxy = _hubConnection.CreateHubProxy(hubId);
            _hubConnection.Start().Wait();
        }

        #endregion

        #region IDataRelayClient<T> Members

        /// <summary>
        ///     Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            _hubConnection.Dispose();
        }

        public void Send(string method, T message)
        {
            _proxy.Invoke(method, message);
        }

        public void Send(string method, IEnumerable<T> messages)
        {
            _proxy.Invoke(method, messages);
        }

        #endregion
    }
}