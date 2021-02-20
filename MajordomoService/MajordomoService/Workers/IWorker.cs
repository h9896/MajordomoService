using MajordomoService.Services;
using NetMQ;
using System;

namespace MajordomoService.Workers
{
    public interface IWorker
    {
        string ID { get; }
        NetMQFrame Identity { get; }
        MicroService Service { get; }
        DateTime Expiry { get; set; }
    }
}
