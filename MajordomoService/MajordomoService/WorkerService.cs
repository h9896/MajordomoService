using System;
using System.Threading.Tasks;
using MajordomoService.Logs;
using NetMQ;
using NetMQ.Sockets;
using System.Threading;
using MajordomoService.Elements;
using System.Reflection;

namespace MajordomoService
{
    public abstract class WorkerService : LogAndUnitService, IWorkerService, IDisposable
    {
        public int HeartbeatLiveliness => _heartbeatLiveliness;
        public string ServiceName => _serviceName;
        public bool IsConnected => _isConnected;
        public bool IsRunning => _isRunning;
        public TimeSpan HeartbeatInterval => _heartbeatInterval;
        public DealerSocket Socket => _socket;
        private const int _heartbeatLiveliness = 3;

        public virtual int _remainHeartbeatCount { get; set; }

        private NetMQQueue<NetMQMessage> _sendToBroker { get; set; }
        private DealerSocket _socket { get; set; }
        private string _brokerAddress { get; set; }
        private string _serviceName { get; set; }
        private byte[] _identity { get; set; }
        private TimeSpan _heartbeatInterval { get; set; }
        private TimeSpan _heartbeatCountInterval { get; set; }
        private bool _isConnected { get; set; }
        private bool _isRunning { get; set; }
        public WorkerService(string brokerAddress, string serviceName, byte[] identity = null) : this()
        {
            if (string.IsNullOrWhiteSpace(brokerAddress))
                throw new ArgumentNullException(nameof(brokerAddress),
                                                 "The address of the broker must not be null, empty or whitespace!");
            if (string.IsNullOrWhiteSpace(serviceName))
                throw new ArgumentNullException(nameof(serviceName),
                                                 "The name of the service must not be null, empty or whitespace!");
            _brokerAddress = brokerAddress;
            _serviceName = serviceName;
            _identity = identity;
        }
        private WorkerService()
        {
            SetTitle("WORKER");
            _isConnected = false;
            _sendToBroker = new NetMQQueue<NetMQMessage>();
            _remainHeartbeatCount = _heartbeatLiveliness;
            _heartbeatInterval = TimeSpan.FromMilliseconds(2500);
            _heartbeatCountInterval = TimeSpan.FromTicks(_heartbeatInterval.Ticks * _heartbeatLiveliness);
        }
        public void SetHeartbeatInterval(TimeSpan interval)
        {
            if (_isConnected)
                throw new InvalidOperationException("Can't change heartbeat interval during running service!");
            _heartbeatInterval = interval;
            _heartbeatCountInterval = TimeSpan.FromTicks(_heartbeatInterval.Ticks * _heartbeatLiveliness);
        }
        public void SetSocket(DealerSocket socket)
        {
            if (_isConnected)
                throw new InvalidOperationException("Can't change socket during running service!");
            _socket = socket;
        }
        public virtual async Task StartService(CancellationToken token)
        {
            if (_socket == null)
                throw new InvalidOperationException("Can't start without dealer socket!");
            if (_isRunning)
                throw new InvalidOperationException("Can't start same worker more than once!");
            if (_identity != null && _identity.Length > 0)
                _socket.Options.Identity = _identity;
            _socket.ReceiveReady += ProcessReceive;
            _socket.Connect(_brokerAddress);
            _isRunning = true;
            _isConnected = true;
            var major = Assembly.GetExecutingAssembly().GetName().Version.Major;
            var minor = Assembly.GetExecutingAssembly().GetName().Version.Minor;
            Log($"MD Worker/{major}.{minor} is active.");
            using (var poller = new NetMQPoller())
            {
                _sendToBroker.ReceiveReady += ProcessSendBroker;
                Send(MDCommand.Ready, _serviceName, null);
                var timer = new NetMQTimer(_heartbeatInterval);
                var countTimer = new NetMQTimer(_heartbeatCountInterval);
                timer.Elapsed += (s, e) => Send(MDCommand.Heartbeat, null, null);
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
        /// <summary>
        /// Send a message to broker
        /// if no message provided create a new empty one
        /// prepend the message with the MDP prologue
        /// </summary>
        /// <param name="mdCommand">MD command</param>
        /// <param name="data">data to be sent</param>
        /// <param name="message">the message to send</param>
        public void Send(MDCommand mdCommand, string data, NetMQMessage message)
        {
            // cmd, null, message      -> [REPLY],<null>,[client adr][e][reply]
            // cmd, string, null       -> [READY],[service name]
            // cmd, null, null         -> [HEARTBEAT]
            var msg = (message == null) ? new NetMQMessage() : message;
            if (data != null) { msg.Push(data); }
            // set command                              ([client adr][e][reply] OR [service]) => [data]
            msg.Push(new[] { (byte)mdCommand });        // [command][data]
            // set Header
            msg.Push(MDConstants.WorkerHeader);         // [header][command][data]
            // set empty frame as separator
            msg.Push(NetMQFrame.Empty);                 // [e][header][command][data]
            _sendToBroker.Enqueue(msg);
            Log($"Enqueue {msg} to broker / Command {mdCommand}");
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
