using System;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Input;
using Windows.Foundation;

namespace AIWF.Native;

public sealed partial class MainWindow
{
    private void InitializeKeyboardAccelerators()
    {
        if (Content is not UIElement rootElement)
        {
            return;
        }

        rootElement.KeyboardAcceleratorPlacementMode = KeyboardAcceleratorPlacementMode.Hidden;

        AddShortcut(rootElement, Windows.System.VirtualKey.S, Windows.System.VirtualKeyModifiers.Control, async (_, args) =>
        {
            if (_activeSection != NavSection.Canvas)
            {
                return;
            }

            args.Handled = true;
            try
            {
                await SaveCanvasSnapshotAsync(showStatus: true);
            }
            catch (Exception ex)
            {
                SetInlineStatus($"保存画布失败：{ex.Message}", InlineStatusTone.Error);
            }
        });

        AddShortcut(rootElement, Windows.System.VirtualKey.O, Windows.System.VirtualKeyModifiers.Control, async (_, args) =>
        {
            if (_activeSection != NavSection.Canvas)
            {
                return;
            }

            args.Handled = true;
            try
            {
                await ReloadCanvasSnapshotAsync(showStatus: true, missingIsError: true);
            }
            catch (Exception ex)
            {
                SetInlineStatus($"加载画布失败：{ex.Message}", InlineStatusTone.Error);
            }
        });

        AddShortcut(rootElement, Windows.System.VirtualKey.N, Windows.System.VirtualKeyModifiers.Control, (_, args) =>
        {
            if (_activeSection != NavSection.Canvas)
            {
                return;
            }

            args.Handled = true;
            CreateNewCanvas();
        });

        AddShortcut(rootElement, Windows.System.VirtualKey.Number0, Windows.System.VirtualKeyModifiers.Control, (_, args) =>
        {
            if (_activeSection != NavSection.Canvas)
            {
                return;
            }

            ResetCanvasView();
            ClampCanvasTransform();
            SetInlineStatus("已重置画布视图。", InlineStatusTone.Success);
            args.Handled = true;
        });

        AddShortcut(rootElement, Windows.System.VirtualKey.Delete, Windows.System.VirtualKeyModifiers.None, (_, args) =>
        {
            if (_activeSection != NavSection.Canvas || IsTextInputFocused())
            {
                return;
            }

            if (!DeleteSelectedConnection())
            {
                DeleteSelectedUserNode();
            }

            args.Handled = true;
        });
    }

    private static void AddShortcut(
        UIElement rootElement,
        Windows.System.VirtualKey key,
        Windows.System.VirtualKeyModifiers modifiers,
        TypedEventHandler<KeyboardAccelerator, KeyboardAcceleratorInvokedEventArgs> invoked)
    {
        var accelerator = new KeyboardAccelerator
        {
            Key = key,
            Modifiers = modifiers
        };
        accelerator.Invoked += invoked;
        rootElement.KeyboardAccelerators.Add(accelerator);
    }

    private void InitializeCanvasKeyStateTracking()
    {
        if (Content is not FrameworkElement rootElement)
        {
            return;
        }

        rootElement.KeyDown += (_, args) =>
        {
            if (args.Key == Windows.System.VirtualKey.Space)
            {
                _isSpaceHeld = true;
                _lastSpaceKeyTickMs = Environment.TickCount64;
                if (_activeSection == NavSection.Canvas && !IsTextInputFocused())
                {
                    args.Handled = true;
                }
            }
        };
        rootElement.KeyUp += (_, args) =>
        {
            if (args.Key == Windows.System.VirtualKey.Space)
            {
                _isSpaceHeld = false;
                _lastSpaceKeyTickMs = Environment.TickCount64;
                if (_activeSection == NavSection.Canvas && !IsTextInputFocused())
                {
                    args.Handled = true;
                }
            }
        };
    }

    private bool IsTextInputFocused()
    {
        var root = Content as FrameworkElement;
        var xamlRoot = root?.XamlRoot;
        if (xamlRoot is null)
        {
            return false;
        }

        var focused = FocusManager.GetFocusedElement(xamlRoot);
        return focused is TextBox
            || focused is PasswordBox
            || focused is AutoSuggestBox
            || focused is ComboBox;
    }
}
