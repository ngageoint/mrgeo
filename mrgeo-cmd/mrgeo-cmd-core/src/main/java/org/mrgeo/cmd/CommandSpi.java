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

/**
 * TODO:  Explain and give an example of constructing an SPI
 */
public abstract class CommandSpi
{
protected CommandSpi()
{

}

/**
 * Return the class name for the Command this SPI supports. The class will be used to construct
 * the command (<code>class.newInstance()</code>) and then the
 * {@link Command#run(String[], org.apache.hadoop.conf.Configuration) Command.run} will be
 * executed.
 *
 * @return <code> Class<? extends Command> </code> Class this command SPI supports.
 */
public abstract Class<? extends Command> getCommandClass();


/**
 * Return the name of the command. The name must be a single word, white space is <i>not</i>
 * supported.
 *
 * @return <code>String</code> The name of the command
 */
public abstract String getCommandName();

/**
 * A short, single-line description of the command. This description is used to describe the
 * command when using the generic help from the {@link MrGeo} command-line.
 *
 * @return <code>String</code> The textual description of the command
 */

public abstract String getDescription();
}
