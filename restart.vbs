' =============================================================================
' Automation for FactoryDataCollector.exe
' Sequence:
'   1. Launch program if needed
'   2. Close possible "App is Already Activated" dialog
'   3. Activate "Machining Monitor" window
'   4. TAB ×7 → ENTER
'   5. TAB ×8 → ENTER
' =============================================================================

Option Explicit

Dim WshShell, exePath, windowTitle, i

Set WshShell = CreateObject("WScript.Shell")

exePath     = """C:\FactoryMonitorSuite\FactoryDataCollector\FactoryDataCollector.exe"""
windowTitle = "Machining Monitor"

' Launch (or focus if already running)
WshShell.Run exePath, 1, False
WScript.Sleep 12000

' Dismiss possible "App is Already Activated" dialog
WshShell.SendKeys "{ENTER}"
WScript.Sleep 1500

' Make sure main window is active
If Not WshShell.AppActivate(windowTitle) Then
    WScript.Sleep 3000
    WshShell.AppActivate windowTitle
End If

WScript.Sleep 1000                    ' стабілізація фокусу

' ────────────────────────────────────────────────
' Перша послідовність: TAB ×7 → ENTER
For i = 1 To 7
    WshShell.SendKeys "{TAB}"
    WScript.Sleep 220                 ' невелика затримка між TAB
Next

WScript.Sleep 600
WshShell.SendKeys "{ENTER}"
WScript.Sleep 1200                    ' пауза після першого ENTER
' ────────────────────────────────────────────────

' ────────────────────────────────────────────────
' Друга послідовність: TAB ×8 → ENTER
For i = 1 To 8
    WshShell.SendKeys "{TAB}"
    WScript.Sleep 220
Next

WScript.Sleep 30000
WshShell.SendKeys "{ENTER}"
' ────────────────────────────────────────────────

' Опціонально: повідомлення про завершення
' MsgBox "Виконано: TAB×7 + ENTER + TAB×8 + ENTER", 64, "Готово"

Set WshShell = Nothing