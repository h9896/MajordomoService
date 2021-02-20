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
    public class UT_WorkerService
    {
        public const string endPoint = "tcp://127.0.0.1";
        public const string port = "5555";
        [Test, Category("NewWorkerService")]
        public void NewWorkerService_Simple_ShouldReturnNewObject()
        {
            var worker = new BasicWorker($"{endPoint}:{port}", "test");
            Assert.That(worker, Is.Not.Null);
            Assert.That(worker.HeartbeatInterval, Is.EqualTo(TimeSpan.FromMilliseconds(2500)));
        }
        [Test, Category("NewWorkerService")]
        public void SetSocket_ChangeSocket_ShouldGetNewSocket()
        {
            using (var socket1 = new DealerSocket())
            using (var socket2 = new DealerSocket())
            using (var worker = new BasicWorker($"{endPoint}:{port}", "test"))
            {
                socket1.Options.Identity = Encoding.UTF8.GetBytes("socket1");
                socket2.Options.Identity = Encoding.UTF8.GetBytes("socket2");
                worker.SetSocket(socket1);
                Assert.That(worker.Socket.Options.Identity, Is.EqualTo(Encoding.UTF8.GetBytes("socket1")));
                worker.SetSocket(socket2);
                Assert.That(worker.Socket.Options.Identity, Is.EqualTo(Encoding.UTF8.GetBytes("socket2")));
            }
        }
        [Test, Category("NewWorkerService")]
        public void SetHeartbeatInterval_ChangeHeartbeatInterval_ShouldReturnNewHeartbeatInterval()
        {
            var worker = new BasicWorker($"{endPoint}:{port}", "test");
            Assert.That(worker.HeartbeatInterval, Is.EqualTo(TimeSpan.FromMilliseconds(2500)));
            worker.SetHeartbeatInterval(new TimeSpan(0, 1, 0));
            Assert.That(worker.HeartbeatInterval, Is.EqualTo(TimeSpan.FromMinutes(1)));
        }
        [Test, Category("NewWorkerService")]
        public void NewWorkerService_InvalidBrokerAddress_ShouldThrowApplicationException()
        {
            // ReSharper disable once ObjectCreationAsStatement
            Assert.Throws<ArgumentNullException>(() => new BasicWorker(string.Empty, "test"));
        }
        [Test, Category("NewWorkerService")]
        public void NewWorkerService_invalidServerName_ShouldThrowApplicationException()
        {
            // ReSharper disable once ObjectCreationAsStatement
            Assert.Throws<ArgumentNullException>(() => new BasicWorker($"{endPoint}:{port}", "   "));
        }
        [Test, Category("StartWorkerService")]
        public void StartService_SendHeartbeat_LogSuccessfulRegistration()
        {
            var log = new List<string>();
            using (var cts = new CancellationTokenSource())
            using (var socket = new DealerSocket())
            using (var worker = new BasicWorker($"{endPoint}:{port}", "test"))
            {
                worker.LogInfoReady += (s, e) => log.Add(e.Info);
                worker.SetSocket(socket);
                worker.SetHeartbeatInterval(TimeSpan.FromMilliseconds(100));
                worker.StartService(cts.Token);
                Thread.Sleep(300);
                cts.Cancel();
                Assert.That(log.Exists(content => content.Contains($"to broker / Command {MDCommand.Heartbeat}")), Is.True);
            }
        }
        [Test, Category("StartWorkerService")]
        public void StartService_SendReady_LogSuccessfulRegistration()
        {
            var log = new List<string>();
            using (var cts = new CancellationTokenSource())
            using (var socket = new DealerSocket())
            using (var worker = new BasicWorker($"{endPoint}:{port}", "test"))
            {
                worker.LogInfoReady += (s, e) => log.Add(e.Info);
                worker.SetSocket(socket);
                worker.SetHeartbeatInterval(TimeSpan.FromMilliseconds(1000));
                worker.StartService(cts.Token);
                Thread.Sleep(300);
                cts.Cancel();
                Assert.That(log.Count(content => content.Contains($"to broker / Command {MDCommand.Ready}")), Is.EqualTo(1));
            }
        }
        [Test, Category("StartWorkerService")]
        public void StartService_WaitHeartbeatAndDisconnect_LogSuccessfulRegistration()
        {
            var log = new List<string>();
            var serviceName = "Test";
            using (var cts = new CancellationTokenSource())
            using (var socket = new DealerSocket())
            using (var internalSocket = new RouterSocket())
            using (var broker = new BasicBroker(new RouterSocket(), internalSocket, new TimeSpan(0, 0, 0, 50)))
            {
                var brokerInternalPort = internalSocket.BindRandomPort(endPoint);
                Task.Run(() => broker.StartService(cts.Token));
                using (var worker = new BasicWorker($"{endPoint}:{brokerInternalPort}", serviceName))
                {
                    worker.LogInfoReady += (s, e) => log.Add(e.Info);
                    worker.SetSocket(socket);
                    worker.SetHeartbeatInterval(TimeSpan.FromMilliseconds(50));
                    worker.StartService(cts.Token);
                    Thread.Sleep(800);
                    Assert.That(worker.IsConnected, Is.EqualTo(false));
                    cts.Cancel();
                    Assert.That(log.Count(content => content.Contains("The service has stopped because of without heartbeat")), Is.EqualTo(1));
                }
            }
        }
        [Test, Category("StartWorkerService")]
        public void StartService_ReceiveFromBroker_LogSuccessfulRegistration()
        {
            var log = new List<string>();
            var serviceName = "Test";
            using (var cts = new CancellationTokenSource())
            using (var socket = new DealerSocket())
            using (var externalSocket = new RouterSocket())
            using (var client = new DealerSocket())
            using (var internalSocket = new RouterSocket())
            using (var broker = new BasicBroker(externalSocket, internalSocket, new TimeSpan(0, 0, 0, 10)))
            {
                var brokerInternalPort = internalSocket.BindRandomPort(endPoint);
                var brokerExternalPort = externalSocket.BindRandomPort(endPoint);
                Task.Run(() => broker.StartService(cts.Token));
                using (var worker = new BasicWorker($"{endPoint}:{brokerInternalPort}", serviceName, Encoding.UTF8.GetBytes("wroker01")))
                {
                    client.Options.Identity = Encoding.UTF8.GetBytes("client01");
                    client.Connect($"{endPoint}:{brokerExternalPort}");
                    worker.LogInfoReady += (s, e) => log.Add(e.Info);
                    worker.SetSocket(socket);
                    worker.SetHeartbeatInterval(TimeSpan.FromMilliseconds(5000));
                    worker.StartService(cts.Token);
                    Thread.Sleep(300);
                    client.TrySendMultipartMessage(GetRequestMsg(serviceName));
                    Thread.Sleep(300);
                    cts.Cancel();
                    Assert.That(log.Count(content => content.Contains($"to broker / Command {MDCommand.Ready}")), Is.EqualTo(1));
                    Assert.That(log.Count(content => content.Contains($"Received the request:") && content.Contains($"client: client01")), Is.EqualTo(1));
                }
            }
        }
        private NetMQMessage GetRequestMsg(string service)
        {
            var msg = new NetMQMessage();
            var request = "This is request frame";
            msg.Push(request);
            msg.Push(service);
            msg.Push(new[] { (byte)MDCommand.Request });
            msg.Push(MDConstants.ClientHeader);
            msg.Push(NetMQFrame.Empty);
            return msg;
        }
    }
}
