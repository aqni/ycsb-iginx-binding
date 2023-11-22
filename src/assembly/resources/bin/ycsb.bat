@REM
@REM Copyright (c) 2012 - 2016 YCSB contributors. All rights reserved.
@REM
@REM Licensed under the Apache License, Version 2.0 (the "License"); you
@REM may not use this file except in compliance with the License. You
@REM may obtain a copy of the License at
@REM
@REM http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing, software
@REM distributed under the License is distributed on an "AS IS" BASIS,
@REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
@REM implied. See the License for the specific language governing
@REM permissions and limitations under the License. See accompanying
@REM LICENSE file.
@REM
@REM -----------------------------------------------------------------------
@REM Control Script for YCSB
@REM
@REM Environment Variable Prerequisites
@REM
@REM   Do not set the variables in this script. Instead put them into a script
@REM   setenv.sh in YCSB_HOME/bin to keep your customizations separate.
@REM
@REM   YCSB_HOME       (Optional) YCSB installation directory.  If not set
@REM                   this script will use the parent directory of where this
@REM                   script is run from.
@REM
@REM   JAVA_HOME       (Required) Must point at your Java Development Kit
@REM                   or Java Runtime Environment installation.
@REM
@REM   JAVA_OPTS       (Optional) Java runtime options used when any command
@REM                   is executed.
@REM
@REM   WARNING!!! YCSB home must be located in a directory path that doesn't
@REM   contain spaces.
@REM

@ECHO OFF
SETLOCAL ENABLEDELAYEDEXPANSION

@REM Only set YCSB_HOME if not already set
PUSHD %~dp0..
IF NOT DEFINED YCSB_HOME SET YCSB_HOME=%CD%
POPD

@REM Ensure that any extra CLASSPATH variables are set via setenv.bat
SET CLASSPATH=

@REM Check if we have a usable JDK
IF "%JAVA_HOME%." == "." GOTO noJavaHome
IF NOT EXIST "%JAVA_HOME%\bin\java.exe" GOTO noJavaHome
GOTO okJava
:noJavaHome
ECHO The JAVA_HOME environment variable is not defined correctly.
GOTO exit
:okJava

@REM Determine YCSB command argument
IF NOT "load" == "%1" GOTO noload
SET YCSB_COMMAND=-load
SET YCSB_CLASS=site.ycsb.Client
GOTO gotCommand
:noload
IF NOT "run" == "%1" GOTO noRun
SET YCSB_COMMAND=-t
SET YCSB_CLASS=site.ycsb.Client
GOTO gotCommand
:noRun
IF NOT "shell" == "%1" GOTO noShell
SET YCSB_COMMAND=
SET YCSB_CLASS=site.ycsb.CommandLine
GOTO gotCommand
:noShell
ECHO [ERROR] Found unknown command '%1'
ECHO [ERROR] Expected one of 'load', 'run', or 'shell'. Exiting.
GOTO exit
:gotCommand

@REM Find binding information
FOR /F "delims=" %%G in (
  'FINDSTR /B "%2:" %YCSB_HOME%\bin\bindings.properties'
) DO SET "BINDING_LINE=%%G"

IF NOT "%BINDING_LINE%." == "." GOTO gotBindingLine
ECHO [ERROR] The specified binding '%2' was not found.  Exiting.
GOTO exit
:gotBindingLine

@REM Pull out binding name and class
FOR /F "tokens=1-2 delims=:" %%G IN ("%BINDING_LINE%") DO (
  SET BINDING_NAME=%%G
  SET BINDING_CLASS=%%H
)

@REM Core libraries
FOR %%F IN (%YCSB_HOME%\lib\*.jar) DO (
  SET CLASSPATH=!CLASSPATH!;%%F%
)

@REM Get the rest of the arguments, skipping the first 2
FOR /F "tokens=2*" %%G IN ("%*") DO (
  SET YCSB_ARGS=%%H
)

@REM Run YCSB
@ECHO ON
"%JAVA_HOME%\bin\java.exe" %JAVA_OPTS% -classpath "%CLASSPATH%" %YCSB_CLASS% %YCSB_COMMAND% -db %BINDING_CLASS% %YCSB_ARGS%
@ECHO OFF

GOTO end

:exit
EXIT /B 1;

:end

