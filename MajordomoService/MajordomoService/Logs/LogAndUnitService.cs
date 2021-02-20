using MajordomoService.Elements;
using NetMQ;
using System;

namespace MajordomoService.Logs
{
    public abstract class LogAndUnitService: ILogAndUnitService
    {
        /// <summary>
        ///     if service has a log message available if fires this event
        /// </summary>
        public event EventHandler<MDLogEventArgs> LogInfoReady;
        /// <summary>
        ///     if service has a error log message available if fires this event
        /// </summary>
        public event EventHandler<MDLogEventArgs> LogErrorReady;
        private string _title { get; set; } = "BROKER";
        public void SetTitle(string title)
        {
            _title = title;
        }
        protected virtual void OnLogInfoReady(MDLogEventArgs e)
        {
            LogInfoReady?.Invoke(this, e);
        }
        protected virtual void OnLogErrorReady(MDLogEventArgs e)
        {
            LogErrorReady?.Invoke(this, e);
        }
        public void Log(string info)
        {
            if (string.IsNullOrWhiteSpace(info))
                return;

            OnLogInfoReady(new MDLogEventArgs { Info = $"[MD {_title}] " + info });
        }
        public void LogError(string error)
        {
            if (string.IsNullOrWhiteSpace(error))
                return;

            OnLogErrorReady(new MDLogEventArgs { Info = $"[MD {_title} Error] " + error });
        }
        /// <summary>
        ///     adds an empty frame and a specified frame to a message
        /// </summary>
        public NetMQMessage Wrap(NetMQFrame frame, NetMQMessage message)
        {
            var result = new NetMQMessage(message);

            if (frame.BufferSize > 0)
            {
                result.Push(NetMQFrame.Empty);
                result.Push(frame);
            }

            return result;
        }
        /// <summary>
        ///     returns the first frame and deletes the next frame if it is empty
        ///
        ///     CHANGES message!
        /// </summary>
        public NetMQFrame UnWrap(NetMQMessage message)
        {
            var frame = message.Pop();

            if (message.First == NetMQFrame.Empty)
                message.Pop();

            return frame;
        }
    }
}
