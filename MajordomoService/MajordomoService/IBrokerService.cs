using MajordomoService.Elements;
using MajordomoService.Services;
using MajordomoService.Workers;
using NetMQ;
using NetMQ.Sockets;
using System;
using System.Collections.Generic;
using System.Threading;

namespace MajordomoService
{
    public interface IBrokerService
    {
        List<Worker> KnownWorkers { get; }
        bool IsRunning { get; }
        RouterSocket ExternalSocket { get; }
        RouterSocket InternalSocket { get;}
        TimeSpan HeartbeatInterval { get; }
        TimeSpan HeartbeatExpiry { get; }
        void StartService(CancellationToken token);
        void ProcessReceivedWorkers(object sender, NetMQSocketEventArgs e);
        void ProcessReceivedClients(object sender, NetMQSocketEventArgs e);
        void SetHeartbeatInterval(TimeSpan interval);
        void WorkerSend(Worker worker, MDCommand command, NetMQMessage message, string option = null);
        void SendDataToWorkers(NetMQMessage msg);
        void SendDataToClients(NetMQMessage msg);
        void RemoveWorker(Worker worker);
        MicroService ServiceRequired(string serviceName);
    }
}
