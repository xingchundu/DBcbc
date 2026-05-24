#!/bin/bash

mvn clean -f pom.xml
mvn compile package -f pom.xml -Dmaven.test.skip=true

CURRENT_DIR=$(pwd);
mv $CURRENT_DIR/dbcbc-web/target/dbcbc-*.zip $CURRENT_DIR