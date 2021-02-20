using MajordomoService.Elements;
using NetMQ;
using NetMQ.Sockets;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace MajordomoService
{
    public interface IWorkerService
    {
        int HeartbeatLiveliness { get; }
        TimeSpan HeartbeatInterval { get; }
        DealerSocket Socket { get; }
        string ServiceName { get; }
        bool IsConnected { get; }
        bool IsRunning { get; }
        int _remainHeartbeatCount { get; set; }
        void SetHeartbeatInterval(TimeSpan interval);
        void SetSocket(DealerSocket socket);
        Task StartService(CancellationToken token);
        void Send(MDCommand mdCommand, string data, NetMQMessage message);
        void ProcessReceive(object sender, NetMQSocketEventArgs e);
    }
}
