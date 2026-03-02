using System.ComponentModel;
using System.Runtime.CompilerServices;

namespace AIWF.Native.ViewModels;

public sealed class MainViewModel : INotifyPropertyChanged
{
    private string _bridgeUrl = "http://127.0.0.1:18081";
    private string _actor = "native";
    private string _owner = "native";
    private string _jobId = "smoke";
    private string _flow = "cleaning";

    public event PropertyChangedEventHandler? PropertyChanged;

    public string BridgeUrl
    {
        get => _bridgeUrl;
        set => SetField(ref _bridgeUrl, value);
    }

    public string Actor
    {
        get => _actor;
        set => SetField(ref _actor, value);
    }

    public string Owner
    {
        get => _owner;
        set => SetField(ref _owner, value);
    }

    public string JobId
    {
        get => _jobId;
        set => SetField(ref _jobId, value);
    }

    public string Flow
    {
        get => _flow;
        set => SetField(ref _flow, value);
    }

    private void SetField<T>(ref T field, T value, [CallerMemberName] string? memberName = null)
    {
        if (Equals(field, value))
        {
            return;
        }

        field = value;
        PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(memberName));
    }
}
