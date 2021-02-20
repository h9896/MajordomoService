using MajordomoService.Workers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MajordomoService.Services
{
    public interface IMicroService
    {
        string Name { get; }
        IEnumerable<Worker> WaitingWorkers { get; }
        void AddWaitingWorker(Worker worker);
        void DeleteWorker(Worker worker);
        Worker GetNextWorker();
    }
}
