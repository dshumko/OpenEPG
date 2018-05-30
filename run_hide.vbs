Dim WshShell
Set WshShell = WScript.CreateObject("WScript.Shell")
WshShell.Run WshShell.CurrentDirectory & "\OpenEPG.cmd", 0, False
