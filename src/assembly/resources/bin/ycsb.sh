#!/bin/sh
#
# Copyright (c) 2012 - 2016 YCSB contributors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License. You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License. See accompanying
# LICENSE file.
#
# -----------------------------------------------------------------------------
# Control Script for YCSB
#
# Environment Variable Prerequisites
#
#   Do not set the variables in this script. Instead put them into a script
#   setenv.sh in YCSB_HOME/bin to keep your customizations separate.
#
#   YCSB_HOME       (Optional) YCSB installation directory.  If not set
#                   this script will use the parent directory of where this
#                   script is run from.
#
#   JAVA_HOME       (Optional) Must point at your Java Development Kit
#                   installation.  If empty, this script tries use the
#                   available java executable.
#
#   JAVA_OPTS       (Optional) Java runtime options used when any command
#                   is executed.
#
#   WARNING!!! YCSB home must be located in a directory path that doesn't
#   contain spaces.
#
#        www.shellcheck.net was used to validate this script

# Cygwin support
CYGWIN=false
case "$(uname)" in
CYGWIN*) CYGWIN=true;;
MINGW*)  CYGWIN=true;;
esac

# Get script path
SCRIPT_DIR=$(dirname "$0" 2>/dev/null)

# Only set YCSB_HOME if not already set
[ -z "$YCSB_HOME" ] && YCSB_HOME=$(cd "$SCRIPT_DIR/.." || exit; pwd)

# Ensure that any extra CLASSPATH variables are set via setenv.sh
CLASSPATH=

# Attempt to find the available JAVA, if JAVA_HOME not set
if [ -z "$JAVA_HOME" ]; then
  JAVA_PATH=$(which java 2>/dev/null)
  if [ "x$JAVA_PATH" != "x" ]; then
    JAVA_HOME=$(dirname "$(dirname "$JAVA_PATH" 2>/dev/null)")
  fi
fi

# If JAVA_HOME still not set, error
if [ -z "$JAVA_HOME" ]; then
  echo "[ERROR] Java executable not found. Exiting."
  exit 1;
fi

# Determine YCSB command argument
if [ "load" = "$1" ] ; then
  YCSB_COMMAND=-load
  YCSB_CLASS=site.ycsb.Client
elif [ "run" = "$1" ] ; then
  YCSB_COMMAND=-t
  YCSB_CLASS=site.ycsb.Client
elif [ "shell" = "$1" ] ; then
  YCSB_COMMAND=
  YCSB_CLASS=site.ycsb.CommandLine
else
  echo "[ERROR] Found unknown command '$1'"
  echo "[ERROR] Expected one of 'load', 'run', or 'shell'. Exiting."
  exit 1;
fi

# Find binding information
BINDING_LINE=$(grep "^$2:" "$YCSB_HOME/bin/bindings.properties" -m 1)

if [ -z "$BINDING_LINE" ] ; then
  echo "[ERROR] The specified binding '$2' was not found.  Exiting."
  exit 1;
fi

# Get binding name and class
BINDING_NAME=$(echo "$BINDING_LINE" | cut -d':' -f1)
BINDING_CLASS=$(echo "$BINDING_LINE" | cut -d':' -f2)

# For Cygwin, ensure paths are in UNIX format before anything is touched
if $CYGWIN; then
  [ -n "$JAVA_HOME" ] && JAVA_HOME=$(cygpath --unix "$JAVA_HOME")
  [ -n "$CLASSPATH" ] && CLASSPATH=$(cygpath --path --unix "$CLASSPATH")
fi

# Core libraries
CLASSPATH="$YCSB_HOME/lib/*"

# For Cygwin, switch paths to Windows format before running java
if $CYGWIN; then
  [ -n "$JAVA_HOME" ] && JAVA_HOME=$(cygpath --unix "$JAVA_HOME")
  [ -n "$CLASSPATH" ] && CLASSPATH=$(cygpath --path --windows "$CLASSPATH")
fi

# Get the rest of the arguments
YCSB_ARGS=$(echo "$@" | cut -d' ' -f3-)

# About to run YCSB
echo "$JAVA_HOME/bin/java $JAVA_OPTS -classpath $CLASSPATH $YCSB_CLASS $YCSB_COMMAND -db $BINDING_CLASS $YCSB_ARGS"

# Run YCSB
# Shellcheck reports the following line as needing double quotes to prevent
# globbing and word splitting.  However, word splitting is the desired effect
# here.  So, the shellcheck error is disabled for this line.
# shellcheck disable=SC2086
"$JAVA_HOME/bin/java" $JAVA_OPTS -classpath "$CLASSPATH" $YCSB_CLASS $YCSB_COMMAND -db $BINDING_CLASS $YCSB_ARGS

