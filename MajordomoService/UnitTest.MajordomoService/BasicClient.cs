using MajordomoService;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NetMQ;
using MajordomoService.Elements;

namespace UnitTest.MajordomoService
{
    public class BasicClient : ClientService
    {
        public BasicClient(string brokerAddress, byte[] identity = null) :base(brokerAddress, identity)
        {

        }
        public override void ProcessReceive(object sender, NetMQSocketEventArgs e)
        {
            var msg = e.Socket.ReceiveMultipartMessage();
            Log($"Received from Broker: {msg}");
            var empty = msg.Pop();                              // msg-> [command][data]
            var cmd = (MDCommand)msg.Pop().Buffer[0];           // msg-> [data]
            _remainHeartbeatCount = HeartbeatLiveliness;
            switch (cmd)
            {
                case MDCommand.Reply:
                    // msg -> [service name][reply]
                    var serviceName = msg.Pop();
                    var reply = msg.Pop();
                    Log($"Received the reply: {reply.ConvertToString()} from service: {serviceName.ConvertToString()}");
                    break;
                case MDCommand.Heartbeat:
                    // msg -> [null]
                    Log("Received heartbeat");
                    break;
                default:
                    LogError("Invalid MDCommand received or message received!");
                    break;
            }

        }
    }
}
