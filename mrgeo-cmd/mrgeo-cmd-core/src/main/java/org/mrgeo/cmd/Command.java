/*
 * Copyright 2009-2016 DigitalGlobe, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 */

package org.mrgeo.cmd;

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
 * Command entry point.
 * <p>
 * args are straight from the main() method.  That means args[0] will contain the name of the command being run.
 *
 * @param args               String[] String array of all arguments.
 * @param conf               Hadoop Configuration object, containing all the default configuration values from a hadoop installation.
 * @param providerProperties properties required for accessing resources while executing
 *                           the command
 * @return int The return status of the command.  Follow typical main() return values: 0 - success,
 * other values - failure (failure code)
 */
public abstract int run(String[] args, Configuration conf, ProviderProperties providerProperties);
}
