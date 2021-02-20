using MajordomoService.Services;
using NetMQ;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MajordomoService.Workers
{
    public class Worker : IWorker
    {
        // the id of the worker as string
        public string ID => _id;
        // identity of worker for routing
        public NetMQFrame Identity => _identity;
        // owing service if known
        public MicroService Service => _serivce;
        public DateTime Expiry { get; set; }
        private string _id { get; set; }
        private NetMQFrame _identity { get; set; }
        private MicroService _serivce { get; set; }
        public Worker(string id, NetMQFrame identity, MicroService service)
        {
            _id = id;
            _identity = identity;
            _serivce = service;
        }
        public override int GetHashCode()
        {
            return (ID + Service.Name).GetHashCode();
        }

        public override string ToString()
        {
            return $"Name = {ID} / Service = {Service.Name} / Expires {Expiry.ToShortTimeString()}";
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(obj, null))
                return false;

            var other = obj as Worker;

            return !ReferenceEquals(other, null) && ID == other.ID && Service.Name == other.Service.Name;
        }
    }
}
