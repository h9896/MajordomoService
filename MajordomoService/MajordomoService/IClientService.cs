using NetMQ;
using NetMQ.Sockets;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace MajordomoService
{
    public interface IClientService
    {
        int HeartbeatLiveliness { get; }
        bool IsConnected { get; }
        bool IsRunning { get; }
        TimeSpan HeartbeatInterval { get; }
        DealerSocket Socket { get; }
        void SetSocket(DealerSocket socket);
        Task StartService(CancellationToken token);
        void Send(string serviceName, NetMQMessage request);
        void ProcessReceive(object sender, NetMQSocketEventArgs e);
    }
}
