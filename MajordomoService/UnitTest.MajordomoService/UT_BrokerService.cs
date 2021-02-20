using System;
using NUnit.Framework;
using NetMQ.Sockets;
using System.Threading;
using System.Collections.Generic;
using System.Threading.Tasks;
using MajordomoService.Elements;
using NetMQ;
using System.Text;
using System.Linq;

namespace UnitTest.MajordomoService
{
    [TestFixture]
    public class UT_BrokerService
    {
        public const string endPoint = "tcp://127.0.0.1";

        [Test, Category("NewBrokerService")]
        public void NewBrokerService_Simple_ShouldReturnNewObject()
        {
            var md = new BasicBroker(new RouterSocket(), new RouterSocket(), new TimeSpan(0, 0, 1));
            Assert.That(md, Is.Not.Null);
            Assert.That(md.ExternalSocket, Is.Not.Null);
            Assert.That(md.InternalSocket, Is.Not.Null);
            Assert.That(md.HeartbeatInterval, Is.EqualTo(TimeSpan.FromSeconds(1)));
        }
        [Test, Category("StartService_SendToWorker")]
        public void StartService_SendHeartbeatToWorker_LogSuccessfulRegistration()
        {
            var log = new List<string>();
            using (var cts = new CancellationTokenSource())
            using (var internalSocket = new RouterSocket())
            using (var broker = new BasicBroker(new RouterSocket(), internalSocket, new TimeSpan(0, 0, 0, 0, 100)))
            {
                internalSocket.BindRandomPort(endPoint);
                broker.LogInfoReady += (s, e) => log.Add(e.Info);
                Task.Run(() => broker.StartService(cts.Token));
                Thread.Sleep(300);
                cts.Cancel();
                Assert.That(log.Exists(content => content == "[MD BROKER] Sent HEARTBEAT to all worker!"), Is.True);
            }
        }
        [Test, Category("StartService_ReceiveFromWorker")]
        public void StartService_ReceiveReadyMessage_LogSuccessfulRegistration()
        {
            var log = new List<string>();
            using (var cts = new CancellationTokenSource())
            using (var internalSocket = new RouterSocket())
            using (var workerSocket = new DealerSocket())
            using (var broker = new BasicBroker(new RouterSocket(), internalSocket, new TimeSpan(0, 0, 0, 0, 100)))
            {
                var port = internalSocket.BindRandomPort(endPoint);
                var serviceName = "Test";
                var identityWork = "worker01";
                broker.LogInfoReady += (s, e) => log.Add(e.Info);
                var msg = GetReadyMsg(serviceName);
                SetIdentityAndConnect(workerSocket, "worker01", $"{endPoint}:{port}");
                Task.Run(() => broker.StartService(cts.Token));
                var result = workerSocket.TrySendMultipartMessage(msg);
                Thread.Sleep(300);
                cts.Cancel();
                Assert.That(result, Is.True);
                Assert.That(log.Count(content => content.Contains($"READY processed. Worker {identityWork} added to service {serviceName}")), Is.EqualTo(1));
                Assert.That(log.Count(content => content.Contains("READY processed")), Is.EqualTo(1));
            }
        }
        [Test, Category("StartService_ReceiveFromWorker")]
        public void StartService_ReceiveDisconnectMessage_LogSuccessfulRegistration()
        {
            var log = new List<string>();

            using (var cts = new CancellationTokenSource())
            using (var internalSocket = new RouterSocket())
            using (var worker = new DealerSocket())
            using (var broker = new BasicBroker(new RouterSocket(), internalSocket, new TimeSpan(0, 0, 1)))
            {
                var identityWork = "TestWorker";
                var serviceName = "Test";
                var port = internalSocket.BindRandomPort(endPoint);
                broker.LogInfoReady += (s, e) => log.Add(e.Info);
                Task.Run(() => broker.StartService(cts.Token));
                SetIdentityAndConnect(worker, identityWork, $"{endPoint}:{port}");
                var msg = GetReadyMsg(serviceName);
                var reasult = worker.TrySendMultipartMessage(msg);
                var disconnect = GetDisconnectMsg(serviceName);
                var disconnectResult = worker.TrySendMultipartMessage(disconnect);
                Thread.Sleep(300);
                cts.Cancel();
                Assert.That(reasult, Is.True);
                Assert.That(disconnectResult, Is.True);
                Assert.That(log.Count(content => content.Contains($"READY processed. Worker {identityWork} added to service {serviceName}")), Is.EqualTo(1));
                Assert.That(log.Count(content => content.Contains($"Disconnect from {identityWork} received.")), Is.EqualTo(1));
            }
        }
        [Test, Category("StartService_ReceiveFromWorker")]
        public void StartService_ReceiveReplyMessageFromWorker_SendTokenToService()
        {
            var log = new List<string>();

            using (var cts = new CancellationTokenSource())
            using (var internalSocket = new RouterSocket())
            using (var worker = new DealerSocket())
            using (var broker = new BasicBroker(new RouterSocket(), internalSocket, new TimeSpan(0, 0, 1)))
            {
                var identityWork = "TestWorker";
                var serviceName = "TestService";
                var clientIdentity = "TestClient";
                worker.Options.Identity = Encoding.UTF8.GetBytes(identityWork);
                var port = internalSocket.BindRandomPort(endPoint);
                broker.LogInfoReady += (s, e) => log.Add(e.Info);
                Task.Run(() => broker.StartService(cts.Token));
                worker.Connect($"{endPoint}:{port}");
                var msg = GetReadyMsg(serviceName);
                var readyReasult = worker.TrySendMultipartMessage(msg);
                var replyReasult = worker.TrySendMultipartMessage(GetReplyMsg(clientIdentity, serviceName));
                Thread.Sleep(300);
                cts.Cancel();
                Assert.That(readyReasult, Is.True);
                Assert.That(replyReasult, Is.True);
                Assert.That(log.Count(content => content.Contains($"READY processed. Worker {identityWork} added to service {serviceName}")), Is.EqualTo(1));
                Assert.That(log.Count(content => content.Contains($"Reply from {identityWork} received and send to {clientIdentity}")), Is.EqualTo(1));
            }
        }
        [Test, Category("StartService_ReceiveFromWorker")]
        public void StartService_ReceiveNotEnoughMessageFromWorker_ErrorLogSuccessfulRegistration()
        {
            var log = new List<string>();

            using (var cts = new CancellationTokenSource())
            using (var forWorker = new RouterSocket())
            using (var worker = new DealerSocket())
            using (var broker = new BasicBroker(new RouterSocket(), forWorker, new TimeSpan(0, 0, 1)))
            {
                var identityWork = "TestWorker";
                worker.Options.Identity = Encoding.UTF8.GetBytes(identityWork);
                var port = forWorker.BindRandomPort(endPoint);
                broker.LogErrorReady += (s, e) => log.Add(e.Info);
                Task.Run(() => broker.StartService(cts.Token));
                worker.Connect($"{endPoint}:{port}");
                var msg = new NetMQMessage();
                msg.Push(MDConstants.WorkerHeader);
                msg.Push(NetMQFrame.Empty);
                worker.TrySendMultipartMessage(msg);
                Thread.Sleep(300);
                cts.Cancel();
                Assert.That(log.Count(content => content.Contains("Message with too few frames received.")), Is.EqualTo(1));
            }
        }
        [Test, Category("StartService_ReceiveFromClient")]
        public void StartService_ReceiveNotEnoughMessageFromClient_ErrorLogSuccessfulRegistration()
        {
            var log = new List<string>();
            using (var cts = new CancellationTokenSource())
            using (var externalSocket = new RouterSocket())
            using (var client = new DealerSocket())
            using (var broker = new BasicBroker(externalSocket, new RouterSocket(), new TimeSpan(0, 0, 1)))
            {
                var identityClient = "TestClient";
                var clientPort = externalSocket.BindRandomPort(endPoint);
                broker.LogErrorReady += (s, e) => log.Add(e.Info);
                Task.Run(() => broker.StartService(cts.Token));
                SetIdentityAndConnect(client, identityClient, $"{endPoint}:{clientPort}");
                var msg = new NetMQMessage();
                msg.Push(MDConstants.ClientHeader);
                msg.Push(NetMQFrame.Empty);
                client.TrySendMultipartMessage(msg);
                Thread.Sleep(300);
                cts.Cancel();
                Assert.That(log.Count(content => content.Contains("Message with too few frames received.")), Is.EqualTo(1));
            }
        }
        [Test, Category("StartService_ReceiveFromClient")]
        public void StartService_ReceiveRequestMessageWithWorker_LogSuccessfulRegistration()
        {
            var log = new List<string>();

            using (var cts = new CancellationTokenSource())
            using (var externalSocket = new RouterSocket())
            using (var internalSocket = new RouterSocket())
            using (var client = new DealerSocket())
            using (var worker = new DealerSocket())
            using (var broker = new BasicBroker(externalSocket, internalSocket, new TimeSpan(0, 0, 1)))
            {
                var identityClient = "TestClient";
                var serviceName = "TestService";
                var identityWorker = "TestWorker";
                var clientPort = externalSocket.BindRandomPort(endPoint);
                var workerPort = internalSocket.BindRandomPort(endPoint);
                broker.LogInfoReady += (s, e) => log.Add(e.Info);
                Task.Run(() => broker.StartService(cts.Token));
                SetIdentityAndConnect(client, identityClient, $"{endPoint}:{clientPort}");
                SetIdentityAndConnect(worker, identityWorker, $"{endPoint}:{workerPort}");
                var readyMsg = GetReadyMsg(serviceName);
                var readyReasult = worker.TrySendMultipartMessage(readyMsg);
                Thread.Sleep(300);
                var msg = GetRequestMsg(serviceName);
                var result = client.TrySendMultipartMessage(msg);
                Thread.Sleep(300);
                cts.Cancel();
                Assert.That(result, Is.True);
                Assert.That(readyReasult, Is.True);
                Assert.That(log.Count(content => content.Contains($"Received from Client:")), Is.EqualTo(1));
                Assert.That(log.Count(content => content.Contains($"Send request to {identityWorker} from {identityClient}")), Is.EqualTo(1));
            }
        }
        [Test, Category("StartService_ReceiveFromClient")]
        public void StartService_ReceiveRequestMessageWithoutWorker_LogSuccessfulRegistration()
        {
            var log = new List<string>();

            using (var cts = new CancellationTokenSource())
            using (var externalSocket = new RouterSocket())
            using (var client = new DealerSocket())
            using (var broker = new BasicBroker(externalSocket, new RouterSocket(), new TimeSpan(0, 0, 1)))
            {
                var identityClient = "TestClient";
                var serviceName = "TestService";
                var port = externalSocket.BindRandomPort(endPoint);
                broker.LogInfoReady += (s, e) => log.Add(e.Info);
                Task.Run(() => broker.StartService(cts.Token));
                SetIdentityAndConnect(client, identityClient, $"{endPoint}:{port}");
                var msg = GetRequestMsg(serviceName);
                var reasult = client.TrySendMultipartMessage(msg);
                Thread.Sleep(300);
                cts.Cancel();
                var reply = client.ReceiveMultipartMessage();
                var ErrMsg = GetErrorMsgFromMDService(reply);
                Assert.That(reasult, Is.True);
                Assert.That(ErrMsg.Contains("ErrorCode:14"), Is.True);
                Assert.That(ErrMsg.Contains("ErrorMsg:There is no worker for the service"), Is.True);
                Assert.That(log.Count(content => content.Contains($"Received from Client:")), Is.EqualTo(1));
                Assert.That(log.Count(content => content.Contains($"The service : {serviceName} is not available now")), Is.EqualTo(1));
            }
        }
        private NetMQMessage GetReadyMsg(string serviceName)
        {
            var msg = new NetMQMessage();
            msg.Push(serviceName);
            msg.Push(new[] { (byte)MDCommand.Ready });
            msg.Push(MDConstants.WorkerHeader);
            msg.Push(NetMQFrame.Empty);
            return msg;
        }
        private NetMQMessage GetDisconnectMsg(string serviceName)
        {
            var msg = new NetMQMessage();
            msg.Push(new[] { (byte)MDCommand.Disconnect });
            msg.Push(MDConstants.WorkerHeader);
            msg.Push(NetMQFrame.Empty);
            return msg;
        }
        private NetMQMessage GetReplyMsg(string clientIdentity, string service)
        {
            var msg = new NetMQMessage();
            var reply = "This is data frame";
            msg.Push(reply);
            msg.Push(NetMQFrame.Empty);
            msg.Push(clientIdentity);
            msg.Push(new[] { (byte)MDCommand.Reply });
            msg.Push(MDConstants.WorkerHeader);
            msg.Push(NetMQFrame.Empty);
            return msg;
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
        private string GetErrorMsgFromMDService(NetMQMessage msg)
        {
            //Frame 0: Empty frame
            //Frame 1: 0x03(one byte, representing Reply)
            //Frame 2: Service name
            //Frame 3: Reply body (opaque binary)
            var empty = msg.Pop();
            var command = (MDCommand)msg.Pop().Buffer[0];
            var service = msg.Pop().ConvertToString();
            var reply = msg.Pop().ConvertToString();
            return reply;
        }
        private void SetIdentityAndConnect(DealerSocket socket, string identity, string endPoint)
        {
            socket.Options.Identity = Encoding.UTF8.GetBytes(identity);
            socket.Connect(endPoint);
        }
    }
}
