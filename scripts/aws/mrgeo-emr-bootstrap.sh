#!/bin/bash
set -e

# The jscience JAR file contains a copy of the opengis classes in org/opengis/metadata/citation
# that conflict with gt-opengis JAR file that MrGeo builds against. To address this problem, we
# rename the jscience JAR file so it doesn't wind up in the Hadoop classpath on the master or
# data nodes.
#mv /usr/share/aws/emr/emrfs/lib/jscience-4.3.1.jar /usr/share/aws/emr/emrfs/lib/jscience-4.3.1.jar.keep
zip -d /usr/share/aws/emr/emrfs/lib/jscience-4.3.1.jar org/opengis/*
#mv /usr/share/aws/emr/lib/jscience-4.3.1.jar /usr/share/aws/emr/lib/jscience-4.3.1.jar.keep
zip -d /usr/share/aws/emr/lib/jscience-4.3.1.jar org/opengis/*
#mv /usr/share/aws/emr/scripts/lib/jscience-4.3.1.jar /usr/share/aws/emr/scripts/lib/jscience-4.3.1.jar.keep
zip -d /usr/share/aws/emr/scripts/lib/jscience-4.3.1.jar org/opengis/*

