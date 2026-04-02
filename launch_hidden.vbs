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
    Dim candidate

    candidate = PreferPythonGuiCommand(ResolvePyLauncherPython())
    If candidate <> "" Then
        ResolvePythonGuiCommand = candidate
        Exit Function
    End If

    candidate = PreferPythonGuiCommand(ResolveWhereExecutable("pythonw.exe"))
    If candidate <> "" Then
        ResolvePythonGuiCommand = candidate
        Exit Function
    End If

    candidate = PreferPythonGuiCommand(ResolveWhereExecutable("python.exe"))
    If candidate <> "" Then
        ResolvePythonGuiCommand = candidate
        Exit Function
    End If

    candidate = ResolveLatestLocalPython()
    If candidate <> "" Then
        ResolvePythonGuiCommand = candidate
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
    If IsAcceptablePythonPath(exePath) Then
        ResolvePyLauncherPython = exePath
    Else
        ResolvePyLauncherPython = ""
    End If
End Function

Function PreferPythonGuiCommand(pyExe)
    Dim pywExe
    pyExe = Trim(pyExe)
    If Not IsAcceptablePythonPath(pyExe) Then
        PreferPythonGuiCommand = ""
        Exit Function
    End If

    pywExe = ReplacePythonExeWithPythonw(pyExe)
    If IsAcceptablePythonPath(pywExe) Then
        PreferPythonGuiCommand = pywExe
        Exit Function
    End If

    PreferPythonGuiCommand = pyExe
End Function

Function ResolveWhereExecutable(commandName)
    Dim execObj, output, exePath
    On Error Resume Next
    Set execObj = shell.Exec("cmd /c where " & commandName)
    If Err.Number <> 0 Then
        Err.Clear
        ResolveWhereExecutable = ""
        Exit Function
    End If
    On Error GoTo 0

    Do While execObj.Status = 0
        WScript.Sleep 50
    Loop

    If execObj.ExitCode <> 0 Then
        ResolveWhereExecutable = ""
        Exit Function
    End If

    output = Trim(execObj.StdOut.ReadAll)
    exePath = FirstLine(output)
    If IsAcceptablePythonPath(exePath) Then
        ResolveWhereExecutable = exePath
    Else
        ResolveWhereExecutable = ""
    End If
End Function

Function ResolveLatestLocalPython()
    Dim rootPath, rootFolder, subFolder, candidate, bestPath, bestDigits, digits, folderName
    rootPath = shell.ExpandEnvironmentStrings("%LocalAppData%") & "\Programs\Python"
    If Not fso.FolderExists(rootPath) Then
        ResolveLatestLocalPython = ""
        Exit Function
    End If

    On Error Resume Next
    Set rootFolder = fso.GetFolder(rootPath)
    If Err.Number <> 0 Then
        Err.Clear
        ResolveLatestLocalPython = ""
        Exit Function
    End If
    On Error GoTo 0

    bestPath = ""
    bestDigits = ""
    For Each subFolder In rootFolder.SubFolders
        folderName = UCase(subFolder.Name)
        digits = DigitsOnly(subFolder.Name)
        If Left(folderName, 6) = "PYTHON" And digits <> "" Then
            candidate = PreferPythonGuiCommand(fso.BuildPath(subFolder.Path, "python.exe"))
            If candidate = "" Then
                candidate = PreferPythonGuiCommand(fso.BuildPath(subFolder.Path, "pythonw.exe"))
            End If
            If candidate <> "" Then
                If bestDigits = "" Or CompareDigitText(digits, bestDigits) > 0 Then
                    bestDigits = digits
                    bestPath = candidate
                End If
            End If
        End If
    Next

    ResolveLatestLocalPython = bestPath
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

Function IsWindowsAppsAlias(pathText)
    Dim normalized
    normalized = LCase(Trim(CStr(pathText)))
    If normalized = "" Then
        IsWindowsAppsAlias = False
        Exit Function
    End If
    IsWindowsAppsAlias = (InStr(normalized, "\microsoft\windowsapps\") > 0)
End Function

Function IsAcceptablePythonPath(pathText)
    pathText = Trim(CStr(pathText))
    If pathText = "" Then
        IsAcceptablePythonPath = False
        Exit Function
    End If
    If IsWindowsAppsAlias(pathText) Then
        IsAcceptablePythonPath = False
        Exit Function
    End If
    IsAcceptablePythonPath = FileExistsSafe(pathText)
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

Function DigitsOnly(text)
    Dim i, ch, result
    result = ""
    For i = 1 To Len(text)
        ch = Mid(text, i, 1)
        If ch >= "0" And ch <= "9" Then
            result = result & ch
        End If
    Next
    DigitsOnly = result
End Function

Function CompareDigitText(leftText, rightText)
    leftText = CStr(leftText)
    rightText = CStr(rightText)
    If Len(leftText) > Len(rightText) Then
        CompareDigitText = 1
        Exit Function
    End If
    If Len(leftText) < Len(rightText) Then
        CompareDigitText = -1
        Exit Function
    End If
    If leftText > rightText Then
        CompareDigitText = 1
    ElseIf leftText < rightText Then
        CompareDigitText = -1
    Else
        CompareDigitText = 0
    End If
End Function

Function Quote(text)
    Quote = Chr(34) & text & Chr(34)
End Function
