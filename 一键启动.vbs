Option Explicit

Dim fso, shell, scriptDir, launcherPath

Set fso = CreateObject("Scripting.FileSystemObject")
Set shell = CreateObject("WScript.Shell")

scriptDir = fso.GetParentFolderName(WScript.ScriptFullName)
launcherPath = fso.BuildPath(scriptDir, "launch_hidden.vbs")

If Not fso.FileExists(launcherPath) Then
    MsgBox "Launcher was not found:" & vbCrLf & launcherPath, vbCritical, "Launch Failed"
    WScript.Quit 1
End If

shell.CurrentDirectory = scriptDir
shell.Run "wscript.exe //nologo " & Quote(launcherPath), 0, False

Function Quote(text)
    Quote = Chr(34) & text & Chr(34)
End Function
