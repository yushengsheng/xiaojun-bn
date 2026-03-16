Option Explicit

Dim fso, shell, scriptDir, targetScript, pythonCmd, launchCmd

Set fso = CreateObject("Scripting.FileSystemObject")
Set shell = CreateObject("WScript.Shell")

scriptDir = fso.GetParentFolderName(WScript.ScriptFullName)
targetScript = FindMainScript(scriptDir)

If targetScript = "" Then
    MsgBox "Main script (*bn.py) was not found." & vbCrLf & scriptDir, vbCritical, "Launch Failed"
    WScript.Quit 1
End If

pythonCmd = ResolvePythonGuiCommand()
If pythonCmd = "" Then
    MsgBox "Could not find a usable pythonw.exe or python.exe.", vbCritical, "Launch Failed"
    WScript.Quit 1
End If

shell.CurrentDirectory = scriptDir
launchCmd = Quote(pythonCmd) & " " & Quote(targetScript)
shell.Run launchCmd, 0, False

Function FindMainScript(folderPath)
    Dim folder, file
    On Error Resume Next
    Set folder = fso.GetFolder(folderPath)
    If Err.Number <> 0 Then
        Err.Clear
        FindMainScript = ""
        Exit Function
    End If
    On Error GoTo 0

    For Each file In folder.Files
        If LCase(Right(file.Name, 5)) = "bn.py" Then
            FindMainScript = file.Path
            Exit Function
        End If
    Next

    FindMainScript = ""
End Function

Function ResolvePythonGuiCommand()
    Dim pyExe, pywExe

    pyExe = ResolvePyLauncherPython()
    If pyExe <> "" Then
        pywExe = ReplacePythonExeWithPythonw(pyExe)
        If pywExe <> "" Then
            ResolvePythonGuiCommand = pywExe
            Exit Function
        End If
        If FileExistsSafe(pyExe) Then
            ResolvePythonGuiCommand = pyExe
            Exit Function
        End If
    End If

    pywExe = shell.ExpandEnvironmentStrings("%LocalAppData%") & "\Programs\Python\Python314\pythonw.exe"
    If FileExistsSafe(pywExe) Then
        ResolvePythonGuiCommand = pywExe
        Exit Function
    End If

    ResolvePythonGuiCommand = ""
End Function

Function ResolvePyLauncherPython()
    Dim execObj, output, exePath
    On Error Resume Next
    Set execObj = shell.Exec("py -3 -c ""import sys; print(sys.executable)""")
    If Err.Number <> 0 Then
        Err.Clear
        ResolvePyLauncherPython = ""
        Exit Function
    End If
    On Error GoTo 0

    Do While execObj.Status = 0
        WScript.Sleep 50
    Loop

    If execObj.ExitCode <> 0 Then
        ResolvePyLauncherPython = ""
        Exit Function
    End If

    output = Trim(execObj.StdOut.ReadAll)
    exePath = FirstLine(output)
    If FileExistsSafe(exePath) Then
        ResolvePyLauncherPython = exePath
    Else
        ResolvePyLauncherPython = ""
    End If
End Function

Function ReplacePythonExeWithPythonw(pyExe)
    Dim folderPath, candidate
    If pyExe = "" Then
        ReplacePythonExeWithPythonw = ""
        Exit Function
    End If
    folderPath = fso.GetParentFolderName(pyExe)
    candidate = fso.BuildPath(folderPath, "pythonw.exe")
    If FileExistsSafe(candidate) Then
        ReplacePythonExeWithPythonw = candidate
    Else
        ReplacePythonExeWithPythonw = ""
    End If
End Function

Function FileExistsSafe(pathText)
    On Error Resume Next
    FileExistsSafe = fso.FileExists(pathText)
    If Err.Number <> 0 Then
        Err.Clear
        FileExistsSafe = False
    End If
    On Error GoTo 0
End Function

Function FirstLine(text)
    Dim normalized, lines
    normalized = Replace(text, vbCrLf, vbLf)
    normalized = Replace(normalized, vbCr, vbLf)
    lines = Split(normalized, vbLf)
    If UBound(lines) >= 0 Then
        FirstLine = Trim(lines(0))
    Else
        FirstLine = ""
    End If
End Function

Function Quote(text)
    Quote = Chr(34) & text & Chr(34)
End Function
