using System;
using MajordomoService;
using NetMQ.Sockets;
using NetMQ;
using MajordomoService.Elements;
using System.Linq;
using MajordomoService.Workers;

namespace UnitTest.MajordomoService
{
    public class BasicBroker : BrokerService
    {
        public BasicBroker(RouterSocket externalSocket, RouterSocket internalSocket, TimeSpan heartbeatInterval) : base(externalSocket, internalSocket, heartbeatInterval)
        {

        }
        public override void ProcessReceivedClients(object sender, NetMQSocketEventArgs e)
        {
            try
            {
                var msg = e.Socket.ReceiveMultipartMessage();
                Log($"Received from Client: {msg}");
                var senderFrame = msg.Pop();               // msg-> [e][protocol header][service or command][data]
                var empty = msg.Pop();                     // msg-> [protocol header][service][data]
                var headerFrame = msg.Pop();               // msg-> [service][data]
                var header = headerFrame.ConvertToString();
                if (header == MDConstants.ClientHeader)
                    ProcessClientMessage(senderFrame, msg);
                else
                    LogError(string.Format("message with invalid protocol header!"));
            }
            catch (Exception err)
            {
                LogError($"[ProcessReceivedClients Fail] {err.Message}");
            }
        }
        public override void ProcessReceivedWorkers(object sender, NetMQSocketEventArgs e)
        {
            try
            {
                var msg = e.Socket.ReceiveMultipartMessage();
                Log($"Received from Worker: {msg}");
                var senderFrame = msg.Pop();               // msg-> [e][protocol header][command][data]
                var empty = msg.Pop();                     // msg-> [protocol header][command][data]
                var headerFrame = msg.Pop();               // msg-> [command][data]
                var header = headerFrame.ConvertToString();
                if (header == MDConstants.WorkerHeader || header == MDConstants.ServiceHeader)
                    ProcessWorkerMessage(senderFrame, msg, header);
                else
                    LogError(string.Format("message with invalid protocol header!"));
            }
            catch (Exception err)
            {
                LogError($"[ProcessReceivedWorkers Fail] {err.Message}");
            }
        }
        #region Process From Workers -> Internal Socket
        private void ProcessWorkerMessage(NetMQFrame sender, NetMQMessage message, string header)
        {
            // should be
            // READY        [md command][service name]
            // REPLY        [md command][client adr][e][reply]
            // HEARTBEAT    [md command]
            // DISCONNECT   [md command]
            if (message.FrameCount < 1)
                throw new ApplicationException("Message with too few frames received.");
            var mdCommand = message.Pop();
            if (mdCommand.BufferSize > 1)
                throw new ApplicationException("The MDCommand frame had more than one byte!");
            var cmd = (MDCommand)mdCommand.Buffer[0];
            var workerId = sender.ConvertToString();       // get the id of the worker sending the message
            var workerIsKnown = KnownWorkers.Any(w => w.ID == workerId);
            switch (cmd)
            {
                case MDCommand.Ready:
                    ProcessReady(sender, message, workerIsKnown, workerId, header);
                    break;
                case MDCommand.Reply:
                    ProcessReply(message, workerIsKnown, workerId);
                    break;
                case MDCommand.Heartbeat:
                    ProcessHeartbeat(workerIsKnown, workerId);
                    break;
                case MDCommand.Disconnect:
                    ProcessDisconnect(workerIsKnown, workerId);
                    break;
                default:
                    LogError("Invalid MDCommand received or message received!");
                    break;
            }
        }
        private void ProcessReady(NetMQFrame sender, NetMQMessage message, bool workerIsKnown, string workerId, string header)
        {
            if (workerIsKnown)
            {
                var worker = KnownWorkers.Find(w => w.ID == workerId);
                RemoveWorker(worker);
                Log($"READY out of sync. Removed worker {workerId}.");
                var msg = new NetMQMessage();
                msg.Push(new[] { (byte)MDCommand.Disconnect });
                var response = Wrap(worker.Identity, msg);
                //Send to Worker.
                SendDataToWorkers(response);
                Log($"Send Disconnect command to worker {workerId}.");
            }
            else
            {
                var serviceName = message.Pop().ConvertToString();
                if (header == MDConstants.ServiceHeader)
                    serviceName = $"{serviceName}-{header}";
                var service = ServiceRequired(serviceName);
                var worker = new Worker(workerId, sender, service);
                AddWorker(worker, service);
                Log($"READY processed. Worker {workerId} added to service {serviceName}");
            }
        }
        private void ProcessReply(NetMQMessage message, bool workerIsKnown, string workerId)
        {
            if (workerIsKnown)
            {
                var worker = KnownWorkers.Find(w => w.ID == workerId);
                var client = UnWrap(message);                   // [reply]
                message.Push(worker.Service.Name);              // [service name][reply]
                message.Push(new[] { (byte)MDCommand.Reply }); // [command][service name][reply]
                var reply = Wrap(client, message);              // [client adr][e][command][service name][reply]
                                                                //Send to Client.
                SendDataToClients(reply);
                Log($"Reply from {workerId} received and send to {client.ConvertToString()} -> {message}");
                //Frame 0: Client Identity
                //Frame 1: Empty frame
                //Frame 2: 0x03(one byte, representing Reply)
                //Frame 3: Service name
                //Frame 4: Reply body (opaque binary)
                AddWorker(worker, worker.Service);
            }
        }
        private void ProcessHeartbeat(bool workerIsKnown, string workerId)
        {
            if (workerIsKnown)
            {
                var worker = KnownWorkers.Find(w => w.ID == workerId);
                worker.Expiry = DateTime.Now + HeartbeatExpiry;
                Log($"Heartbeat from {workerId} received.");
            }
        }
        private void ProcessDisconnect(bool workerIsKnown, string workerId)
        {
            if (workerIsKnown)
            {
                var worker = KnownWorkers.Find(w => w.ID == workerId);
                RemoveWorker(worker);
                Log($"Disconnect from {workerId} received.");
            }
        }
        #endregion
        #region Process From Clients -> External Socket
        private void ProcessClientMessage(NetMQFrame sender, NetMQMessage message)
        {
            // REQUEST [mdp command][service name][request]
            // Heartbeat [mdp command]
            if (message.FrameCount < 1)
                throw new ApplicationException("Message with too few frames received.");
            var mdCommand = message.Pop();
            if (mdCommand.BufferSize > 1)
                throw new ApplicationException("The MDCommand frame had more than one byte!");
            var cmd = (MDCommand)mdCommand.Buffer[0];
            switch (cmd)
            {
                case MDCommand.Request:
                    ProcessRequest(sender, message);
                    break;
                case MDCommand.Heartbeat:
                    var data = new NetMQMessage();
                    data.Push(new[] { (byte)MDCommand.Heartbeat });
                    var response = Wrap(sender, data);
                    SendDataToClients(response);
                    break;
                default:
                    break;
            }
            
        }
        private void ProcessRequest(NetMQFrame sender, NetMQMessage message)
        {
            if (message.FrameCount < 2)
                throw new ArgumentException("The message is malformed!");
            var serviceName = message.Pop().ConvertToString();      // [request]
            var r = message.Pop();                                  // None
            string clientIdentity = sender.ConvertToString();
            message.Push(r.ToByteArray());                          // [request]
            var request = Wrap(sender, message);                    // [CLIENT ADR][e][request]
            var service = ServiceRequired(serviceName);
            var worker = service.GetNextWorker();
            if (worker != null)
            {
                ProcessRequestWithWorkers(sender, worker, request);
            }
            else
            {
                ProcessRequestWithoutWorkers(sender, serviceName);
            }
        }
        private void ProcessRequestWithWorkers(NetMQFrame sender, Worker worker, NetMQMessage request)
        {
            WorkerSend(worker, MDCommand.Request, request);
            Log($"Send request to {worker.ID} from {sender.ConvertToString()}");
        }
        private void ProcessRequestWithoutWorkers(NetMQFrame sender, string serviceName)
        {
            //REPLY
            //see the https://grpc.github.io/grpc/core/md_doc_statuscodes.html for error code
            var ErrorMsg = "There is no worker for the service";
            var msg = $"ErrorCode:{14}, ErrorMsg:{ErrorMsg}";
            var reply = new NetMQMessage();
            reply.Push(msg);
            reply.Push(serviceName);
            reply.Push(new[] { (byte)MDCommand.Reply });
            var response = Wrap(sender, reply);

            //Send to Client
            SendDataToClients(response);
            //Frame 0: Client Identity
            //Frame 1: Empty frame
            //Frame 2: 0x03(one byte, representing Reply)
            //Frame 3: Service name
            //Frame 4: Reply body (opaque binary)
            //Add log
            Log($"The service : {serviceName} is not available now");
        }
        #endregion
    }
}
