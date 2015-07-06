/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
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
 */

package org.mrgeo.mapreduce.formats;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.Job;

import java.util.Set;

public class MrsPyramidInputFormatUtils {
	private static final String PREFIX = "MrsPyramidInputFormat";
	
	public static final String INPUTS = PREFIX + ".Inputs";
	//public static final String OUTPUT = PREFIX + ".Output";
//	public static final String BOUNDS = PREFIX + ".Bounds";

//	public static final String INCLUDE_EMPTY_TILES = PREFIX + ".IncludeEmptyTiles";
 
//	public static void addInput(final Job job, final Path path)
//	{
//		final Configuration conf = job.getConfiguration();
//
//		String existing = conf.get(INPUTS, "");
//		if (existing.length() > 0)
//		{
//			existing += ",";
//		}
//		existing += path.toString();
//	}

//	public static void includeEmptyTiles(final Job job, final boolean includeEmpty)
//	{
//		job.getConfiguration().setBoolean(INCLUDE_EMPTY_TILES, includeEmpty);
//	}

//	public static void setInputs(final Job job, final List<String> inputPaths)
//	{
//		setInputs(job, StringUtils.join(inputPaths, ","));
//	}

//	public static void setInputs(final Job job, final Path... inputPaths)
//	{
//		setInputs(job, StringUtils.join(inputPaths, ","));
//	}

	public static void setInputs(final Job job, final Set<String> inputPaths)
	{
		setInputs(job, StringUtils.join(inputPaths, ","));
	}

	public static void setInputs(final Job job, final String commaSeparatedPaths)
	{
		job.getConfiguration().set(INPUTS, commaSeparatedPaths);
	}

//	public static void setOutput(final Job job, final String output)
//	{
//		final Configuration conf = job.getConfiguration();
//
//		conf.set(OUTPUT, output);
//	}
}
