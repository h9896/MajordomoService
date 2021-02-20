namespace MajordomoService.Elements
{
    public enum MDCommand
    {
        Kill = 0x00,
        Ready = 0x01,
        Request = 0x02,
        Reply = 0x03,
        Heartbeat = 0x04,
        Disconnect = 0x05,
        Token = 0x06
    }
}
