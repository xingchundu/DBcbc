@echo off
rem Wrapper so "startup.cmd" works the same as startup.bat (e.g. double-click or `startup.cmd`)
call "%~dp0startup.bat" %*
