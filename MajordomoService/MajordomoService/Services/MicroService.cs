using MajordomoService.Workers;
using System.Collections.Generic;

namespace MajordomoService.Services
{
    public class MicroService: IMicroService
    {
        public string Name => _name;
        public IEnumerable<Worker> WaitingWorkers => _waitingWorkers.ToArray();
        private string _name { get; set; }
        private List<Worker> _workers { get; set; }            // list of known and active worker for this service 
        private List<Worker> _waitingWorkers { get; set; }     // queue of workers waiting for requests FIFO!
        public MicroService(string name) : this()
        {
            _name = name;
        }
        private MicroService()
        {
            _workers = new List<Worker>();
            _waitingWorkers = new List<Worker>();
        }
        public void AddWaitingWorker(Worker worker)
        {
            if (!IsKnown(worker))
                _workers.Add(worker);

            if (!IsWaiting(worker))
            {
                // add to the end of the list
                // oldest is at the beginning of the list
                _waitingWorkers.Add(worker);
            }
        }
        public void DeleteWorker(Worker worker)
        {
            if (IsKnown(worker.ID))
                _workers.Remove(worker);

            if (IsWaiting(worker))
                _waitingWorkers.Remove(worker);
        }
        public Worker GetNextWorker()
        {
            var worker = _waitingWorkers.Count == 0 ? null : _waitingWorkers[0];

            if (worker != null)
                _waitingWorkers.Remove(worker);

            return worker;
        }
        private bool IsKnown(Worker worker) { return _workers.Contains(worker); }
        private bool IsKnown(string workerName) { return _workers.Exists(w => w.ID == workerName); }
        private bool IsWaiting(Worker worker) { return _waitingWorkers.Contains(worker); }

    }
}
