#!/bin/sh
EXEC_ARGS=$@
mvn exec:java -Dexec.mainClass=org.jasypt.intf.cli.JasyptPBEStringEncryptionCLI -Dexec.args="$EXEC_ARGS"
