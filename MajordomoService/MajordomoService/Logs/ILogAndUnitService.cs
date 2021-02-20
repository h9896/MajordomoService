using MajordomoService.Elements;
using NetMQ;
using System;

namespace MajordomoService.Logs
{
    public interface ILogAndUnitService
    {
        event EventHandler<MDLogEventArgs> LogInfoReady;
        event EventHandler<MDLogEventArgs> LogErrorReady;
        void SetTitle(string title);
        void Log(string info);
        void LogError(string error);
        NetMQMessage Wrap(NetMQFrame frame, NetMQMessage message);
        NetMQFrame UnWrap(NetMQMessage message);
    }
}
