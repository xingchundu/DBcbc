@echo off

echo "Clean Project ..."
call mvn clean -f pom.xml

echo "Build Project ..."
call mvn compile package -f pom.xml -D"maven.test.skip=true"

set CP_PATH=%~dp0
move %CP_PATH%dbcbc-web\target\dbcbc-*.zip %CP_PATH%

:exit