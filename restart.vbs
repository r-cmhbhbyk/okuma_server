Option Explicit

Dim WshShell, exePath
Dim objWMI, colProcesses, objProcess
Dim processName, isRunning

Set WshShell = CreateObject("WScript.Shell")
Set objWMI   = GetObject("winmgmts:\\.\root\cimv2")

exePath     = """C:\FactoryMonitorSuite\FactoryDataCollector\FactoryDataCollector.exe"""
processName = "FactoryDataCollector.exe"

isRunning = False
Set colProcesses = objWMI.ExecQuery("SELECT * FROM Win32_Process WHERE Name='" & processName & "'")

For Each objProcess In colProcesses
    isRunning = True
    objProcess.Terminate
Next

If isRunning Then
    WScript.Sleep 2000
End If

WshShell.Run exePath, 1, False

Set objWMI   = Nothing
Set WshShell = Nothing