@echo off
setlocal EnableExtensions

rem Console UTF-8 so Chinese log lines match JVM -Dfile.encoding=UTF-8 (default cmd is GBK)
chcp 65001 >nul

rem set up environment for Java (set system env JAVA_HOME, or uncomment below)
rem set "JAVA_HOME=D:\java\jdk1.8.0_202"

rem Resolve install root (parent of bin); pushd collapses ".." to a real path
pushd "%~dp0\.."
set "DBS_HOME=%CD%"
popd
echo DBS_HOME=%DBS_HOME%

if "%JAVA_HOME%"=="" (
    echo ERROR: JAVA_HOME is not set. Edit this script or set the JAVA_HOME environment variable.
    goto :fail
)

if not exist "%JAVA_HOME%\bin\java.exe" (
    echo ERROR: java.exe not found under "%JAVA_HOME%\bin"
    goto :fail
)

cd /d "%DBS_HOME%"

set "JAVA_CMD=%JAVA_HOME%\bin\java.exe"
set "CLASSPATH=%DBS_HOME%\lib\*"

rem G1GC: works on Java 8+; CMS/ParNew removed in newer JDKs (e.g. 11+)
set SERVER_OPTS=-Xms3800m -Xmx3800m -Xss512k -XX:MetaspaceSize=192m -XX:+DisableAttachMechanism
rem set IPv4
rem set SERVER_OPTS=%SERVER_OPTS% -Djava.net.preferIPv4Stack=true -Djava.net.preferIPv4Addresses

set ENCRYPT_FILE=%DBS_HOME%\bin\libDBSyncer.dll
if exist "%ENCRYPT_FILE%" (
    set SERVER_OPTS=%SERVER_OPTS% -agentpath:%ENCRYPT_FILE%
)

rem Java 9+ no longer supports java.ext.dirs; use classpath (Java 8 accepts lib\* on Windows too)
set SERVER_OPTS=%SERVER_OPTS% -Dspring.config.location=%DBS_HOME%\conf\application.properties
set SERVER_OPTS=%SERVER_OPTS% -DLOG_HOME=%DBS_HOME%\logs
set SERVER_OPTS=%SERVER_OPTS% -Dsun.stdout.encoding=UTF-8 -Dsun.stderr.encoding=UTF-8 -Dfile.encoding=UTF-8 -Duser.dir=%DBS_HOME%
set SERVER_OPTS=%SERVER_OPTS% -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:ParallelGCThreads=4 -XX:+DisableExplicitGC
rem JDK 9+ removed PrintGCDateStamps; avoid deprecated GC log flags on newer JDKs
set SERVER_OPTS=%SERVER_OPTS% -verbose:gc
set SERVER_OPTS=%SERVER_OPTS% -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=%DBS_HOME%\logs -XX:ErrorFile=%DBS_HOME%\logs\hs_err.log

echo %SERVER_OPTS%
"%JAVA_CMD%" -cp "%CLASSPATH%" %SERVER_OPTS% org.dbsyncer.web.Application
if errorlevel 1 goto :fail
goto :eof

:fail
echo.
pause
exit /b 1