@echo off
setlocal
set "APP_HOME=%~dp0"
set "JAR=%APP_HOME%gradle\wrapper\gradle-wrapper.jar"
set "URL=https://raw.githubusercontent.com/gradle/gradle/v9.6.1/gradle/wrapper/gradle-wrapper.jar"
set "SHA=497c8c2a7e5031f6aa847f88104aa80a93532ec32ee17bdb8d1d2f67a194a9c7"
if not exist "%JAR%" powershell -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference='Stop'; $t='%JAR%.tmp'; Invoke-WebRequest -UseBasicParsing -Uri '%URL%' -OutFile $t; if ((Get-FileHash -Algorithm SHA256 $t).Hash.ToLowerInvariant() -ne '%SHA%') { Remove-Item $t; throw 'Checksum mismatch' }; Move-Item -Force $t '%JAR%'"
if errorlevel 1 exit /b 1
powershell -NoProfile -ExecutionPolicy Bypass -Command "if ((Get-FileHash -Algorithm SHA256 '%JAR%').Hash.ToLowerInvariant() -ne '%SHA%') { throw 'Checksum mismatch' }"
if errorlevel 1 exit /b 1
if defined JAVA_HOME (set "JAVA_EXE=%JAVA_HOME%\bin\java.exe") else (set "JAVA_EXE=java.exe")
"%JAVA_EXE%" -Xmx64m -Xms64m -classpath "%JAR%" org.gradle.wrapper.GradleWrapperMain %*
endlocal
