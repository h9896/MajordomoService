using MajordomoService;
using System;
using NetMQ;
using MajordomoService.Elements;

namespace UnitTest.MajordomoService
{
    public class BasicWorker: WorkerService
    {
        public BasicWorker(string brokerAddress, string serviceName, byte[] identity = null) :base(brokerAddress, serviceName, identity)
        {

        }

        public override void ProcessReceive(object sender, NetMQSocketEventArgs e)
        {
            var msg = e.Socket.ReceiveMultipartMessage();
            Log($"Received from Broker: {msg}");
            var empty = msg.Pop();                      // msg-> [protocol header][command][data]
            var hearder = msg.Pop();                    // msg-> [command][data]
            if (hearder.ConvertToString() != MDConstants.WorkerHeader)
                throw new ApplicationException("Ther protocol header is not worker header.");
            var cmd = (MDCommand)msg.Pop().Buffer[0];   // msg-> [data]
            _remainHeartbeatCount = HeartbeatLiveliness;
            switch (cmd)
            {
                case MDCommand.Request:
                    // msg -> [client adr][e][request]
                    var client = msg.Pop();
                    var requestFrame = UnWrap(msg);
                    Log($"Received the request: {requestFrame.ConvertToString()} from client: {client.ConvertToString()}");
                    break;
                case MDCommand.Heartbeat:
                    // msg -> [null]
                    Send(MDCommand.Heartbeat, null, null);
                    break;
                default:
                    LogError("Invalid MDCommand received or message received!");
                    break;
            }
        }
    }
}
