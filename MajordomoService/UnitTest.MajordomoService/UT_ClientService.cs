using System;
using System.Text;
using System.Collections.Generic;
using NUnit.Framework;
using System.Linq;
using NetMQ.Sockets;
using System.Threading;
using MajordomoService.Elements;
using System.Threading.Tasks;
using NetMQ;

namespace UnitTest.MajordomoService
{
    [TestFixture]
    public class UT_ClientService
    {
        public const string endPoint = "tcp://127.0.0.1";
        public const string port = "5555";
        [Test, Category("NewClientService")]
        public void NewClientService_Simple_ShouldReturnNewObject()
        {
            var client = new BasicClient($"{endPoint}:{port}");
            Assert.That(client, Is.Not.Null);
            Assert.That(client.HeartbeatInterval, Is.EqualTo(TimeSpan.FromMilliseconds(2500)));
        }
        [Test, Category("NewClientService")]
        public void SetSocket_ChangeSocket_ShouldGetNewSocket()
        {
            using (var socket1 = new DealerSocket())
            using (var socket2 = new DealerSocket())
            using (var client = new BasicClient($"{endPoint}:{port}"))
            {
                socket1.Options.Identity = Encoding.UTF8.GetBytes("socket1");
                socket2.Options.Identity = Encoding.UTF8.GetBytes("socket2");
                client.SetSocket(socket1);
                Assert.That(client.Socket.Options.Identity, Is.EqualTo(Encoding.UTF8.GetBytes("socket1")));
                client.SetSocket(socket2);
                Assert.That(client.Socket.Options.Identity, Is.EqualTo(Encoding.UTF8.GetBytes("socket2")));
            }
        }
        [Test, Category("NewClientService")]
        public void SetHeartbeatInterval_ChangeHeartbeatInterval_ShouldReturnNewHeartbeatInterval()
        {
            var client = new BasicClient($"{endPoint}:{port}");
            Assert.That(client.HeartbeatInterval, Is.EqualTo(TimeSpan.FromMilliseconds(2500)));
            client.SetHeartbeatInterval(new TimeSpan(0, 1, 0));
            Assert.That(client.HeartbeatInterval, Is.EqualTo(TimeSpan.FromMinutes(1)));
        }
        [Test, Category("NewClientService")]
        public void NewWorkerService_InvalidBrokerAddress_ShouldThrowApplicationException()
        {
            // ReSharper disable once ObjectCreationAsStatement
            Assert.Throws<ArgumentNullException>(() => new BasicClient(string.Empty));
        }
        [Test, Category("StartClientService")]
        public void StartService_Start_LogSuccessfulRegistration()
        {
            var log = new List<string>();
            using (var cts = new CancellationTokenSource())
            using (var socket = new DealerSocket())
            using (var client = new BasicClient($"{endPoint}:{port}"))
            {
                client.LogInfoReady += (s, e) => log.Add(e.Info);
                client.SetSocket(socket);
                client.SetHeartbeatInterval(TimeSpan.FromMilliseconds(100));
                client.StartService(cts.Token);
                Thread.Sleep(300);
                cts.Cancel();
                Assert.That(log.Exists(content => content.Contains("Starting to listen for incoming messages ...")), Is.True);
            }
        }
        [Test, Category("StartClientService")]
        public void StartService_SendHeartbeat_LogSuccessfulRegistration()
        {
            var log = new List<string>();
            using (var cts = new CancellationTokenSource())
            using (var socket = new DealerSocket())
            using (var client = new BasicClient($"{endPoint}:{port}"))
            {
                client.LogInfoReady += (s, e) => log.Add(e.Info);
                client.SetSocket(socket);
                client.SetHeartbeatInterval(TimeSpan.FromMilliseconds(100));
                client.StartService(cts.Token);
                Thread.Sleep(300);
                cts.Cancel();
                Assert.That(log.Exists(content => content.Contains("Enqueue heartbeat to broker")), Is.True);
            }
        }
        [Test, Category("StartClientService")]
        public void StartService_ReplyFromBroker_LogSuccessfulRegistration()
        {
            var log = new List<string>();
            var serviceName = "Test";
            using (var cts = new CancellationTokenSource())
            using (var socket = new DealerSocket())
            using (var externalSocket = new RouterSocket())
            using (var internalSocket = new RouterSocket())
            using (var broker = new BasicBroker(externalSocket, internalSocket, new TimeSpan(0, 0, 0, 10)))
            {
                var brokerExternalPort = externalSocket.BindRandomPort(endPoint);
                Task.Run(() => broker.StartService(cts.Token));
                using (var client = new BasicClient($"{endPoint}:{brokerExternalPort}", Encoding.UTF8.GetBytes("wroker01")))
                {
                    client.LogInfoReady += (s, e) => log.Add(e.Info);
                    client.SetSocket(socket);
                    client.SetHeartbeatInterval(TimeSpan.FromMilliseconds(5000));
                    client.StartService(cts.Token);
                    Thread.Sleep(300);
                    var msg = new NetMQMessage();
                    var request = "This is request frame";
                    msg.Push(request);
                    client.Send(serviceName, msg);
                    Thread.Sleep(300);
                    cts.Cancel();
                    Assert.That(log.Count(content => content.Contains("Received from Broker:") && content.Contains("There is no worker for the service")), Is.EqualTo(1));
                    Assert.That(log.Count(content => content.Contains("Received the reply") && content.Contains($"from service: {serviceName}")), Is.EqualTo(1));
                }
            }
        }
    }
}
