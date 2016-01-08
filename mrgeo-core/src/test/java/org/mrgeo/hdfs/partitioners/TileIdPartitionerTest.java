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

package org.mrgeo.hdfs.partitioners;

import junit.framework.Assert;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.tile.PartitionerSplit;
import org.mrgeo.hdfs.tile.SplitInfo;
import org.mrgeo.hdfs.tile.Splits;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.HadoopUtils;

import java.io.IOException;
import java.net.URI;
import java.util.Random;

@SuppressWarnings("static-method")
public class TileIdPartitionerTest extends LocalRunnerTest {

@Rule public TemporaryFolder folder = new TemporaryFolder();

static Long[] generated = new Long[]{1L, 2L, 5L, 10L, 15L, 20L, 30L, 50L};

class TestGenerator implements SplitGenerator
{
  @Override
  public SplitInfo[] getSplits()
  {
    PartitionerSplit.PartitionerSplitInfo splits[] = new PartitionerSplit.PartitionerSplitInfo[generated.length];
    for (int i = 0; i < splits.length; i++)
    {
      splits[i] = new PartitionerSplit.PartitionerSplitInfo(generated[i], i);
    }

    return splits;
  }

  @Override
  public SplitInfo[] getPartitions()
  {
    return getSplits();
  }
}

private static Path inputHdfs;
private static Path outputHdfs;

private static Path allonesPath;

@BeforeClass
public static void init() throws IOException
{
  inputHdfs = TestUtils.composeInputHdfs(TileIdPartitionerTest.class, true);
  outputHdfs = TestUtils.composeOutputHdfs(TileIdPartitionerTest.class);

//    String allones = "all-ones";
//    System.out.println(Defs.INPUT + " | " + inputHdfs.toString() + " | " + allones);
//
//    HadoopFileUtils.copyToHdfs(Defs.INPUT, inputHdfs, allones);
//
//    allonesPath = new Path(inputHdfs, allones);
}

@Before
public void setUp() throws IOException
{
}

@Test
@Category(UnitTest.class)
public void setSplitFile() throws IOException
{
  String file = new Path(outputHdfs, testname.getMethodName()).toString();
  Job job = Job.getInstance(conf);

  TileIdPartitioner.setSplitFile(file, job);

  conf = job.getConfiguration();

  Assert.assertEquals("TileIdPartitioner.splitFile not set", file, conf.get("TileIdPartitioner.splitFile", file));
  Assert.assertFalse("TileIdPartitioner.useDistributedCache should not be set",
      conf.getBoolean("TileIdPartitioner.useDistributedCache", false));

  URI files[] = job.getCacheFiles();
  Assert.assertNull("Cache files should not be set", files);
}

@Test
@Category(UnitTest.class)
public void setSplitFileNonLocal() throws IOException
{
  conf = HadoopUtils.createConfiguration(); // force local mode off...

  String file = new Path(outputHdfs, testname.getMethodName()).toString();
  Job job = Job.getInstance(conf);

  TileIdPartitioner.setSplitFile(file, job);

  conf = job.getConfiguration();

  Assert.assertEquals("TileIdPartitioner.splitFile not set",
      file, conf.get("TileIdPartitioner.splitFile", null));
  Assert.assertTrue("TileIdPartitioner.useDistributedCache should be set",
      conf.getBoolean("TileIdPartitioner.useDistributedCache", false));

  URI files[] = job.getCacheFiles();
  Assert.assertEquals("Cache files should have 1 file", 1, files.length);

  Assert.assertEquals("Cache file name wrong", file, files[0].toString());
}

@Test
@Category(UnitTest.class)
public void setupPartitionerLocal() throws IOException
{
  Job job = Job.getInstance(conf, testname.getMethodName());
  FileSystem fs = HadoopFileUtils.getFileSystem(conf);

  SplitGenerator sg = new TestGenerator();

  Path splitfile = TileIdPartitioner.setup(job, sg);

  Assert.assertEquals("Bad number of reducers", 1, job.getNumReduceTasks());
  Assert.assertNull("Splitfile should be null", splitfile);

}

@Test
@Category(UnitTest.class)
public void setupPartitioner() throws IOException
{
  conf = HadoopUtils.createConfiguration();

  Job job = Job.getInstance(conf, testname.getMethodName());
  conf = job.getConfiguration();

  FileSystem fs = HadoopFileUtils.getFileSystem(conf);

  SplitGenerator sg = new TestGenerator();

  Path splitfile = TileIdPartitioner.setup(job, sg);

  Assert.assertTrue("Reducers should be greater than zero", job.getNumReduceTasks() > 0);
  Assert.assertNotNull("Splitfile should not be null", splitfile);

  Assert.assertEquals("TileIdPartitioner.splitFile not set",
      splitfile.toString(), conf.get("TileIdPartitioner.splitFile", null));
  Assert.assertTrue("TileIdPartitioner.useDistributedCache should be set",
      conf.getBoolean("TileIdPartitioner.useDistributedCache", false));

  URI files[] = job.getCacheFiles();
  Assert.assertEquals("Cache files should have 1 file", 1, files.length);

  Assert.assertEquals("Cache file name wrong", splitfile.toString(), files[0].toString());

  Assert.assertTrue("Partition Splits directory does not exist", fs.exists(splitfile));
  Assert.assertTrue("Partition Splits file does not exist",
      fs.exists(new Path(splitfile, PartitionerSplit.SPLIT_FILE)));

  Splits splits = new PartitionerSplit();
  splits.readSplits(splitfile);

  PartitionerSplit.PartitionerSplitInfo[] si =
      (PartitionerSplit.PartitionerSplitInfo[]) splits.getSplits();

  Assert.assertEquals("Splits length not correct", generated.length, si.length);
  for (int i = 0; i < generated.length; i++)
  {
    Assert.assertEquals("Splits entry not correct", generated[i].longValue(), si[i].getTileId());
  }

  fs.delete(splitfile, true);
}

@Test
@Category(UnitTest.class)
public void getSetConf() throws IOException
{
  TileIdPartitioner partitioner = new TileIdPartitioner();
  Assert.assertNull("Configuration should initialize to null", partitioner.getConf());
  partitioner.setConf(conf);
  Assert.assertEquals("Confugration objects don't match", conf, partitioner.getConf());
}

@Test
@Category(UnitTest.class)
public void getPartition() throws IOException
{
  conf = HadoopUtils.createConfiguration();

  Job job = Job.getInstance(conf, testname.getMethodName());
  conf = job.getConfiguration();

  FileSystem fs = HadoopFileUtils.getFileSystem(conf);

  SplitGenerator sg = new TestGenerator();

  Path splitfile = TileIdPartitioner.setup(job, sg);

  Assert.assertTrue("Reducers should be greater than zero", job.getNumReduceTasks() > 0);
  Assert.assertNotNull("Splitfile should not be null", splitfile);

  TileIdPartitioner<TileIdWritable, RasterWritable> partitioner =
      new TileIdPartitioner<TileIdWritable, RasterWritable>();

  partitioner.setConf(conf);

  TileIdWritable key = new TileIdWritable();
  RasterWritable value = new RasterWritable();

  int partitions = generated.length;
  int partition;

  Random rand = new Random();
  for (int i = 0; i < 1000; i++)
  {
    long test = rand.nextInt(generated[generated.length - 1].intValue());
    long testPartition = findSplit(generated, test);

    key.set(test);

    // test the 3 ways of getting a partition
    partition = partitioner.getPartition(key, value, partitions);
    Assert.assertEquals("Splits entry not correct", testPartition, partition);

    partition = partitioner.getPartition(key);
    Assert.assertEquals("Splits entry not correct", testPartition, partition);
  }

  fs.delete(splitfile, true);
}

private long findSplit(Long[] splits, long value)
{
  for (int i = 0; i < splits.length; i++)
  {
    if (value <= splits[i])
    {
      return i;
    }
  }
  return -1;
}

//	@Test
//	@Category(UnitTest.class)
//	public void testNonExistentSplitFile() throws URISyntaxException
//	{
//		File file = new File(new java.net.URI(smallElevationImage));
//		boolean imageExists = file.exists();
//		Assert.assertEquals(true, imageExists);
//
//		// Make sure neither splits file exists in the source...
//		String splitPath = "file://" + file.getAbsolutePath() + "/splits.txt";
//		File splitFile = new File(splitPath);
//		boolean splitExists = splitFile.exists();
//		Assert.assertEquals(false, splitExists);
//
//    splitPath = "file://" + file.getAbsolutePath() + "/splits";
//    splitFile = new File(splitPath);
//    splitExists = splitFile.exists();
//    Assert.assertEquals(false, splitExists);
//
//		int numEntries = read(Long.MIN_VALUE, Long.MAX_VALUE);
//		Assert.assertEquals(numEntries,12);
//	}
//
//	@Test
//	@Category(UnitTest.class)
//	public void testFindPartition() {
//		// tests to see if we have two partitions - 10 and 20, then keys 1, 10, 11, 20, 21 are
//		// assigned to partitions 0, 0, 1, 1, and 2
//		// this covers all cases of keys on the split points, between split points, etc.
//
//		// start with two partitions
//		TileIdWritable[] array = new TileIdWritable[]{new TileIdWritable(10),new TileIdWritable(20)};
//
//		// left extreme is assigned to 0
//		Assert.assertEquals(0, findPartition(new TileIdWritable(1),array));
//
//		// on the first split point is assigned to 0
//		Assert.assertEquals(0, findPartition(new TileIdWritable(10),array));
//
//		// between two split points is assigned to 1
//		Assert.assertEquals(1, findPartition(new TileIdWritable(11),array));
//
//		// on the second split point is assigned to 1
//		Assert.assertEquals(1, findPartition(new TileIdWritable(20),array));
//
//		// right extreme is assigned to 2
//		Assert.assertEquals(2, findPartition(new TileIdWritable(21),array));
//	}
//
//	@Test
//	@Category(UnitTest.class)
//	public void testSetSplitFileWithScheme() throws URISyntaxException, IOException
//	{
//		// has three splits, so four part directories
//		final int numSplits = 3;
//		String splitFilePath = allOnesImage + "/partitions";
//		java.net.URI u = new java.net.URI(splitFilePath);
//		File splitFile = new File(u);
//		boolean splitFileExists = splitFile.exists();
//		Assert.assertEquals(true, splitFileExists);
//
//		final String splitFilePathWithScheme = "file://" + splitFile.getAbsolutePath();
//
//    Job job = new Job(conf);
//    conf = job.getConfiguration();
//
//    TileIdPartitioner<?, ?> partitioner = new TileIdPartitioner<Object, Object>();
//		partitioner.setConf(conf);
//
//		TileIdPartitioner.setSplitFile(splitFilePathWithScheme, job);
//
//		TileIdWritable key = new TileIdWritable();
//
//		key.set(208787);
//		Assert.assertEquals(0, partitioner.getPartition(key, null, numSplits));
//
//		key.set(208789);
//		Assert.assertEquals(0, partitioner.getPartition(key, null, numSplits));
//
//		key.set(209811);
//		Assert.assertEquals(1, partitioner.getPartition(key, null, numSplits));
//
//		key.set(210835);
//		Assert.assertEquals(2, partitioner.getPartition(key, null, numSplits));
//
//		key.set(211859);
//		Assert.assertEquals(3, partitioner.getPartition(key, null, numSplits));
//
//		// beyond image, but still in the last partition
//		key.set(211862);
//		Assert.assertEquals(3, partitioner.getPartition(key, null, numSplits));
//
//	}
//
//	private static int findPartition(TileIdWritable key, TileIdWritable[] array) {
//		// find the bin for the range, and guarantee it is positive
//		int index = Arrays.binarySearch(array, key);
//		index = index < 0 ? (index + 1) * -1 : index;
//
//		return index;
//	}
//
//	private int rread(long start, long end) {
//		TileIdWritable startKey = new TileIdWritable(start);
//		TileIdWritable endKey = new TileIdWritable(end);
//		int numEntries = 0;
//
//    KVIterator<TileIdWritable, Raster> iter = reader.get(startKey, endKey);
//
//    while (iter.hasNext())
//    {
//      numEntries++;
//      iter.next();
//    }
//
//		return numEntries;
//	}
}
