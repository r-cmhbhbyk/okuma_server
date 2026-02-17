' =============================================================================
' Automation for FactoryDataCollector.exe
' Sequence:
'   1. If process is running → kill it
'   2. If not running → launch it
'   3. Activate "Machining Monitor" window
'   4. TAB ×7 → ENTER
' =============================================================================

Option Explicit

Dim WshShell, exePath, windowTitle, i
Dim objWMI, colProcesses, objProcess
Dim processName, isRunning

Set WshShell = CreateObject("WScript.Shell")
Set objWMI   = GetObject("winmgmts:\\.\root\cimv2")

exePath     = """C:\FactoryMonitorSuite\FactoryDataCollector\FactoryDataCollector.exe"""
windowTitle = "Machining Monitor"
processName = "FactoryDataCollector.exe"

' Перевіряємо чи процес запущено
isRunning = False
Set colProcesses = objWMI.ExecQuery("SELECT * FROM Win32_Process WHERE Name='" & processName & "'")

For Each objProcess In colProcesses
    isRunning = True
    objProcess.Terminate
Next

If isRunning Then
    ' Процес був запущений — чекаємо поки завершиться
    WScript.Sleep 2000
End If

' В обох випадках — запускаємо програму
WshShell.Run exePath, 1, False
WScript.Sleep 12000

' Активуємо вікно "Machining Monitor"
If Not WshShell.AppActivate(windowTitle) Then
    WScript.Sleep 3000
    WshShell.AppActivate windowTitle
End If

WScript.Sleep 1000  ' стабілізація фокусу

' TAB ×7 → ENTER
For i = 1 To 6
    WshShell.SendKeys "{TAB}"
    WScript.Sleep 220
Next

WScript.Sleep 600
WshShell.SendKeys "{ENTER}"

Set objWMI   = Nothing
Set WshShell = Nothing