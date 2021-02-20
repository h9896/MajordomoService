using MajordomoService.Elements;
using MajordomoService.Logs;
using NetMQ;
using NetMQ.Sockets;
using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace MajordomoService
{
    public abstract class ClientService : LogAndUnitService, IClientService, IDisposable
    {
        public int HeartbeatLiveliness => _heartbeatLiveliness;
        public bool IsConnected => _isConnected;
        public bool IsRunning => _isRunning;
        public TimeSpan HeartbeatInterval => _heartbeatInterval;
        public DealerSocket Socket => _socket;
        private const int _heartbeatLiveliness = 3;
        private string _brokerAddress { get; set; }
        public virtual int _remainHeartbeatCount { get; set; }
        private byte[] _identity { get; set; }
        private bool _isConnected { get; set; }
        private bool _isRunning { get; set; }
        private TimeSpan _heartbeatInterval { get; set; }
        private TimeSpan _heartbeatCountInterval { get; set; }
        private NetMQQueue<NetMQMessage> _sendToBroker { get; set; }
        private DealerSocket _socket { get; set; }

        public ClientService(string brokerAddress, byte[] identity = null) : this()
        {
            if (string.IsNullOrWhiteSpace(brokerAddress))
                throw new ArgumentNullException(nameof(brokerAddress),
                                                 "The address of the broker must not be null, empty or whitespace!");
            _brokerAddress = brokerAddress;
            _identity = identity;
        }
        private ClientService()
        {
            SetTitle("CLIENT");
            _isConnected = false;
            _sendToBroker = new NetMQQueue<NetMQMessage>();
            _remainHeartbeatCount = _heartbeatLiveliness;
            _heartbeatInterval = TimeSpan.FromMilliseconds(2500);
            _heartbeatCountInterval = TimeSpan.FromTicks(_heartbeatInterval.Ticks * _heartbeatLiveliness);
        }
        public void SetSocket(DealerSocket socket)
        {
            if (_isConnected)
                throw new InvalidOperationException("Can't change socket during running service!");
            _socket = socket;
        }
        public void SetHeartbeatInterval(TimeSpan interval)
        {
            if (_isConnected)
                throw new InvalidOperationException("Can't change heartbeat interval during running service!");
            _heartbeatInterval = interval;
            _heartbeatCountInterval = TimeSpan.FromTicks(_heartbeatInterval.Ticks * _heartbeatLiveliness);
        }
        public virtual async Task StartService(CancellationToken token)
        {
            if (_socket == null)
                throw new InvalidOperationException("Can't start without dealer socket!");
            if (_isRunning)
                throw new InvalidOperationException("Can't start same client more than once!");
            if (_identity != null && _identity.Length > 0)
                _socket.Options.Identity = _identity;
            _socket.ReceiveReady += ProcessReceive;
            _socket.Connect(_brokerAddress);
            _isRunning = true;
            _isConnected = true;
            var major = Assembly.GetExecutingAssembly().GetName().Version.Major;
            var minor = Assembly.GetExecutingAssembly().GetName().Version.Minor;
            Log($"MD Client/{major}.{minor} is active.");
            using (var poller = new NetMQPoller())
            {
                _sendToBroker.ReceiveReady += ProcessSendBroker;
                var timer = new NetMQTimer(_heartbeatInterval);
                var countTimer = new NetMQTimer(_heartbeatCountInterval);
                timer.Elapsed += (s, e) => SendHeartbeat();
                countTimer.Elapsed += (s, e) => CountTimer_Elapsed(s, e);
                poller.Add(countTimer);
                poller.Add(timer);
                poller.Add(_socket);
                poller.Add(_sendToBroker);
                Log("Starting to listen for incoming messages ...");
                await Task.Factory.StartNew(poller.Run, token);
                Log("... Stopped!");
                poller.Remove(countTimer);
                poller.Remove(timer);
                poller.Remove(_socket);
                poller.Remove(_sendToBroker);
                // unregister event handler
                _socket.ReceiveReady -= ProcessReceive;
                _sendToBroker.ReceiveReady -= ProcessSendBroker;
            }
            _socket.Disconnect(_brokerAddress);
            _isConnected = false;
            _isRunning = false;
        }
        public void Send(string serviceName, NetMQMessage request)
        {
            if (string.IsNullOrWhiteSpace(serviceName))
                throw new ApplicationException("serviceName must not be empty or null.");

            if (ReferenceEquals(request, null))
                throw new ApplicationException("the request must not be null");
            var message = new NetMQMessage(request);
            message.Push(serviceName);
            message.Push(new[] { (byte)MDCommand.Request });
            message.Push(MDConstants.ClientHeader);
            message.Push(NetMQFrame.Empty);
            _sendToBroker.Enqueue(message);
            Log($"Enqueue {message} to service {serviceName}");
        }
        public abstract void ProcessReceive(object sender, NetMQSocketEventArgs e);
        private void ProcessSendBroker(object sender, NetMQQueueEventArgs<NetMQMessage> e)
        {
            NetMQMessage msg = e.Queue.Dequeue();
            bool result = _socket.TrySendMultipartMessage(msg);
            Log($"Send to Broker -> {result}, {msg}");
        }
        private void CountTimer_Elapsed(object sender, NetMQTimerEventArgs e)
        {
            if (_remainHeartbeatCount > 0)
                _remainHeartbeatCount -= 1;
            else
            {
                if (_isConnected)
                {
                    _socket.ReceiveReady -= ProcessReceive;
                    _sendToBroker.ReceiveReady -= ProcessSendBroker;
                    _socket.Disconnect(_brokerAddress);
                    _isConnected = false;
                    Log("The service has stopped because of without heartbeat");
                }
            }
        }
        private void SendHeartbeat()
        {
            var msg = new NetMQMessage();
            msg.Push(new[] { (byte)MDCommand.Heartbeat });
            msg.Push(MDConstants.ClientHeader);
            msg.Push(NetMQFrame.Empty);
            _sendToBroker.Enqueue(msg);
            Log($"Enqueue heartbeat to broker");
        }
        #region IDisposable

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _isConnected = false;
                _socket = null;
            }
            // get rid of unmanaged resources
        }

        #endregion IDisposable
    }
}
