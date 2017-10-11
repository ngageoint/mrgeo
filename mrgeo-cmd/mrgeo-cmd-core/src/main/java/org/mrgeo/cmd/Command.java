/*
 * Copyright 2009-2017. DigitalGlobe, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.mrgeo.cmd;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.mrgeo.data.ProviderProperties;

/**
 * Command is the abstract base class for all mrgeo pluggable commands.  At run-time, all classes
 * extending Command will be discovered using Java's ServiceProvider interface and the use of
 * {@link CommandSpi}.  The only entry point into commands is the {@link Command#run(String[], Configuration) run} method.
 * <p>
 * TODO: Add an example in Javadocs
 */
public abstract class Command
{
protected Command()
{
}

/**
 * The command should add the command-line options it supports to the options
 * passed in.
 *
 * @param options
 */
public void addOptions(Options options) {}

/**
 * Subclasses should return the string that is displayed to the user when a command-line
 * parsing error occurs. It should return something like "mycommand <options>". Information
 * about the available options will be included automatically (based on the Options assigned
 * in addOptions).
 *
 * @return
 */
public abstract String getUsage();

/**
 * Sub-classes override this to perform their processing. If there are command-line arguments
 * that are incorrect, throw a ParseException with the description ofthe problem. It
 * will be displayed to the user along with the usage help for the command.
 *
 * @param cmdLine
 * @param conf
 * @param providerProperties
 * @return
 */
public abstract int run(CommandLine cmdLine, Configuration conf,
                        ProviderProperties providerProperties) throws ParseException;
}
