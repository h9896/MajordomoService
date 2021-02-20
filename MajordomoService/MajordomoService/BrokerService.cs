using NetMQ;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MajordomoService.Elements;
using MajordomoService.Workers;
using MajordomoService.Services;
using NetMQ.Sockets;
using System.Threading;
using System.Reflection;
using MajordomoService.Logs;

namespace MajordomoService
{
    public abstract class BrokerService: LogAndUnitService, IBrokerService, IDisposable
    {
        public List<Worker> KnownWorkers => _knownWorkers;
        public bool IsRunning => _isRunning;
        public RouterSocket ExternalSocket => _externalSocket;
        public RouterSocket InternalSocket => _internalSocket;
        public TimeSpan HeartbeatInterval => _heartbeatInterval;
        public TimeSpan HeartbeatExpiry => _heartbeatExpiry;
        private RouterSocket _externalSocket { get; set; }
        private RouterSocket _internalSocket { get; set; }
        private TimeSpan _heartbeatExpiry { get; set; }
        private TimeSpan _heartbeatInterval { get; set; }
        private readonly List<Worker> _knownWorkers;
        private readonly List<MicroService> _services;
        private NetMQQueue<NetMQMessage> _sendForClients { get; set; }
        private NetMQQueue<NetMQMessage> _sendForWorkers { get; set; }
        private bool _isRunning { get; set; }
        private readonly object _syncRoot = new object();
        public BrokerService(RouterSocket externalSocket, RouterSocket internalSocket, TimeSpan heartbeatInterval) : this()
        {
            _externalSocket = externalSocket;
            _internalSocket = internalSocket;
            _heartbeatInterval = heartbeatInterval;
            _heartbeatExpiry = TimeSpan.FromTicks(heartbeatInterval.Ticks * 3);
        }
        private BrokerService()
        {
            _knownWorkers = new List<Worker>();
            _services = new List<MicroService>();
            _sendForClients = new NetMQQueue<NetMQMessage>();
            _sendForWorkers = new NetMQQueue<NetMQMessage>();
            _isRunning = false;
        }
        public void SetHeartbeatInterval(TimeSpan interval)
        {
            if (_isRunning)
                throw new InvalidOperationException("Can't change heartbeat interval during running service!");
            _heartbeatInterval = interval;
            _heartbeatExpiry = TimeSpan.FromTicks(interval.Ticks * 3);
        }
        public virtual void StartService(CancellationToken token)
        {
            if (_isRunning)
                throw new InvalidOperationException("Can't start same broker more than once!");
            _isRunning = true;
            var major = Assembly.GetExecutingAssembly().GetName().Version.Major;
            var minor = Assembly.GetExecutingAssembly().GetName().Version.Minor;
            Log($"MD Broker/{major}.{minor} is active.");
            using (var pollerForClinets = new NetMQPoller())
            using (var pollerForWorkers = new NetMQPoller())
            {
                _internalSocket.ReceiveReady += ProcessReceivedWorkers;
                _externalSocket.ReceiveReady += ProcessReceivedClients;
                _sendForWorkers.ReceiveReady += ProcessSendToWorkers;
                _sendForClients.ReceiveReady += ProcessSendToClients;
                var timer = new NetMQTimer(_heartbeatInterval);
                timer.Elapsed += (s, e) => SendHeartbeat();
                pollerForWorkers.Add(_internalSocket);
                pollerForWorkers.Add(timer);
                pollerForWorkers.Add(_sendForWorkers);
                pollerForClinets.Add(_externalSocket);
                pollerForClinets.Add(_sendForClients);

                Log("Starting to listen for incoming messages ...");

                Task.Factory.StartNew(pollerForClinets.Run, token);
                Task.Factory.StartNew(pollerForWorkers.Run, token).Wait();

                Log("... Stopped!");

                pollerForWorkers.Remove(_internalSocket);
                pollerForWorkers.Remove(timer);
                pollerForWorkers.Remove(_sendForWorkers);
                pollerForClinets.Remove(_externalSocket);
                pollerForClinets.Remove(_sendForClients);

                _internalSocket.ReceiveReady -= ProcessReceivedWorkers;
                _externalSocket.ReceiveReady -= ProcessReceivedClients;
                _sendForWorkers.ReceiveReady -= ProcessSendToWorkers;
                _sendForClients.ReceiveReady -= ProcessSendToClients;
            }
            _isRunning = false;
        }
        /// <summary>
        ///     expect from
        ///     Worker  ->  [sender adr][e][protocol header][mdp command][reply]
        /// </summary>
        public abstract void ProcessReceivedWorkers(object sender, NetMQSocketEventArgs e);
        /// <summary>
        ///     expect from
        ///     Client  ->  [sender adr][e][protocol header][mdp command][service name][request]
        /// </summary>
        public abstract void ProcessReceivedClients(object sender, NetMQSocketEventArgs e);
        /// <summary>
        ///     sends a message to a specific worker with a specific command
        ///     and add an option option
        /// </summary>
        public void WorkerSend(Worker worker, MDCommand command, NetMQMessage message, string option = null)
        {
            var msg = message ?? new NetMQMessage();

            // stack protocol envelope to start of message
            if (!ReferenceEquals(option, null))
                msg.Push(option);

            msg.Push(new[] { (byte)command });
            msg.Push(MDConstants.WorkerHeader);
            // stack routing envelope
            var request = Wrap(worker.Identity, msg);

            Log($"Sending {request}");
            //Send to worker request
            SendDataToWorkers(request);
            //Frame 0: Worker Identity
            //Frame 1: Empty frame
            //Frame 2: Header
            //Frame 3: 0x02(one byte, representing REQUEST)
            //Frame 4: Client address (envelope stack)
            //Frame 5: Empty(zero bytes, envelope delimiter)
            //Frame 6: Request body (opaque binary)
        }
        public void SendDataToWorkers(NetMQMessage msg)
        {
            Log($"Enqueue {msg} to Worker");
            _sendForWorkers.Enqueue(msg);
        }
        public void SendDataToClients(NetMQMessage msg)
        {
            Log($"Enqueue {msg} to Client");
            _sendForClients.Enqueue(msg);
        }
        /// <summary>
        ///     This method deletes any idle workers that haven't pinged us in a
        ///     while. We hold workers from oldest to most recent so we can stop
        ///     scanning whenever we find a live worker. This means we'll mainly stop
        ///     at the first worker, which is essential when we have large numbers of
        ///     workers (we call this method in our critical path)
        /// </summary>
        /// <remarks>
        ///     we must use a lock to guarantee that only one thread will have
        ///     access to the purging at a time!
        /// </remarks>
        private void Purge()
        {
            Log("Start purging for all services");

            lock (_syncRoot)
            {
                foreach (var service in _services)
                {
                    foreach (var worker in service.WaitingWorkers)
                    {
                        if (DateTime.UtcNow < worker.Expiry)
                            // we found the first woker not expired in that service
                            // any following worker will be younger -> we're done for the service
                            break;
                        RemoveWorker(worker);
                    }
                }
            }
        }
        /// <summary>
        ///     removes the worker from the known worker list and
        ///     from the service it refers to if it exists and
        ///     potentially from the waiting list therein
        /// </summary>
        /// <param name="worker"></param>
        public void RemoveWorker(Worker worker)
        {
            if (_services.Contains(worker.Service))
            {
                var service = _services.Find(s => s.Equals(worker.Service));
                service.DeleteWorker(worker);
                Log($"Removed worker {worker.ID} from service {service.Name}");
            }

            _knownWorkers.Remove(worker);
            Log($"Removed {worker.ID} from known worker.");
        }
        /// <summary>
        ///     locates service by name or creates new service if there
        ///     is no service available with that name
        /// </summary>
        /// <param name="serviceName">the service requested</param>
        /// <returns>the requested service object</returns>
        public MicroService ServiceRequired(string serviceName)
        {
            if (_services.Exists(s => s.Name == serviceName))
            {
                return _services.Find(s => s.Name == serviceName);
            }
            else
            {
                var svc = new MicroService(serviceName);
                _services.Add(svc);
                Log($"Added {svc.Name} to services list.");
                return svc;
            }
        }
        /// <summary>
        ///     adds the worker to the service and the known worker list
        ///     if not already known
        ///     it dispatches pending requests for this service as well
        /// </summary>
        public void AddWorker(Worker worker, MicroService service)
        {
            worker.Expiry = DateTime.Now + _heartbeatExpiry;
            if (!_knownWorkers.Contains(worker))
            {
                _knownWorkers.Add(worker);
                Log($"Added {worker.ID} to known worker.");
            }

            service.AddWaitingWorker(worker);
            Log($"Added {worker.ID} to waiting worker in service {service.Name}.");
        }
        private void SendHeartbeat()
        {
            Purge();

            foreach (var worker in _knownWorkers)
                WorkerSend(worker, MDCommand.Heartbeat, null);
            Log("Sent HEARTBEAT to all worker!");
        }
        private void ProcessSendToWorkers(object sender, NetMQQueueEventArgs<NetMQMessage> e)
        {
            NetMQMessage msg = e.Queue.Dequeue();
            bool result = _internalSocket.TrySendMultipartMessage(msg);
            Log($"Send to Workers -> {result}, {msg}");
        }
        private void ProcessSendToClients(object sender, NetMQQueueEventArgs<NetMQMessage> e)
        {
            NetMQMessage msg = e.Queue.Dequeue();
            bool result = _externalSocket.TrySendMultipartMessage(msg);
            Log($"Send to Clients -> {result}, {msg}");
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
                _isRunning = false;
            }
            // get rid of unmanaged resources
        }

        #endregion IDisposable
    }
}
