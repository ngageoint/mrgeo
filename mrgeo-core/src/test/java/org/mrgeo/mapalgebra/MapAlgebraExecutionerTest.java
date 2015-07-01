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

package org.mrgeo.mapalgebra;

import org.apache.hadoop.fs.Path;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mrgeo.core.Defs;
import org.mrgeo.image.MrsImage;
import org.mrgeo.image.MrsImagePyramid;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.mapreduce.formats.TileClusterInfo;
import org.mrgeo.mapreduce.job.JobCancelledException;
import org.mrgeo.mapreduce.job.JobFailedException;
import org.mrgeo.mapreduce.job.JobListener;
import org.mrgeo.progress.Progress;
import org.mrgeo.progress.ProgressHierarchy;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.junit.IntegrationTest;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.LocalRunnerTest;
import org.mrgeo.test.TestUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

@SuppressWarnings("static-method")
public class MapAlgebraExecutionerTest extends LocalRunnerTest
{
  @Rule public TestName testName = new TestName();

  private static final String PREPARE_ACTION = "prepare";
  private static final String BUILD_ACTION = "build";
  private static final String POST_BUILD_ACTION = "postBuild";

  private static Path inputHdfs;
  private static Path outputHdfs;

  private static Path allonesPath;

  private static String copy;

  MapAlgebraExecutioner executioner;
  MapOp mapop;

  @BeforeClass
  public static void init() throws IOException
  {
    inputHdfs = TestUtils.composeInputHdfs(MapAlgebraExecutionerTest.class, true);
    outputHdfs = TestUtils.composeOutputHdfs(MapAlgebraExecutionerTest.class);

    String allones = "all-ones";
    System.out.println(Defs.INPUT + " | " + inputHdfs.toString() + " | " + allones);

    HadoopFileUtils.copyToHdfs(Defs.INPUT, inputHdfs, allones);

    allonesPath = new Path(inputHdfs, allones);

    copy = "[" + HadoopFileUtils.unqualifyPath(allonesPath) + "]";
  }

  @AfterClass
  public static void exit() throws Exception
  {
    inputHdfs = null;
    outputHdfs = null;

    allonesPath = null;

    copy = null;
  }

  @Before
  public void setUp() throws Exception
  {
    System.out.println("setup");
    executioner = new MapAlgebraExecutioner();
        
    final MapAlgebraParser uut = new MapAlgebraParser(conf, "", null);
    mapop = uut.parse(copy);
    MapAlgebraExecutioner.setOverallTileClusterInfo(mapop, new TileClusterInfo());

    executioner.setRoot(mapop);
    executioner.setOutputName(new Path(outputHdfs, testName.getMethodName()).toString());

    System.out.println("setup complete");
  }

  public class MapOpActionListener
  {
    private List<String> actions = new ArrayList<String>();
    private List<String> actionIds = new ArrayList<String>();

    public void addAction(String id, String action)
    {
      actions.add(action);
      actionIds.add(id);
    }

    public int size()
    {
      return actions.size();
    }

    public String getActionId(int index)
    {
      return actionIds.get(index);
    }

    public String getAction(int index)
    {
      return actions.get(index);
    }
  }

  public class DeferredMapOp1 extends MapOp implements DeferredExecutor, OutputProducer
  {
    private MapOpActionListener listener;
    private String id;
    private List<MapOp> inputs = new ArrayList<MapOp>();

    public DeferredMapOp1(String id, MapOpActionListener listener)
    {
      this.id = id;
      this.listener = listener;
    }

    @Override
    public void prepare(Progress p)
    {
      listener.addAction(id, PREPARE_ACTION);
    }

    @Override
    public String getOperationId()
    {
      return DeferredMapOp1.class.getCanonicalName();
    }

    @Override
    public void addInput(MapOp n) throws IllegalArgumentException
    {
      inputs.add(n);
    }

    @Override
    public List<MapOp> getInputs()
    {
      return inputs;
    }

    @Override
    public void build(Progress p) throws IOException, JobFailedException, JobCancelledException
    {
      listener.addAction(id, BUILD_ACTION);
    }

    @Override
    public void postBuild(Progress p, boolean buildPyramid) throws IOException, JobFailedException,
        JobCancelledException
    {
      listener.addAction(id, POST_BUILD_ACTION);
    }

    @Override
    public void setOutputName(String output)
    {
    }

    @Override
    public String getOutputName()
    {
      return "dontCare";
    }

    @Override
    public String resolveOutputName() throws IOException
    {
      return "dontCare";
    }

    @Override
    public void moveOutput(String toName) throws IOException
    {
    }
  }

  public class DeferredMapOp2 extends MapOp implements DeferredExecutor, OutputProducer
  {
    private MapOpActionListener listener;
    private String id;
    private List<MapOp> inputs = new ArrayList<MapOp>();

    public DeferredMapOp2(String id, MapOpActionListener listener)
    {
      this.id = id;
      this.listener = listener;
    }

    @Override
    public void prepare(Progress p)
    {
      listener.addAction(id, PREPARE_ACTION);
    }

    @Override
    public String getOperationId()
    {
      return DeferredMapOp2.class.getCanonicalName();
    }

    @Override
    public void addInput(MapOp n) throws IllegalArgumentException
    {
      inputs.add(n);
    }

    @Override
    public List<MapOp> getInputs()
    {
      return inputs;
    }

    @Override
    public void build(Progress p) throws IOException, JobFailedException, JobCancelledException
    {
      listener.addAction(id, BUILD_ACTION);
    }

    @Override
    public void postBuild(Progress p, boolean buildPyramid) throws IOException, JobFailedException,
        JobCancelledException
    {
      listener.addAction(id, POST_BUILD_ACTION);
    }

    @Override
    public void setOutputName(String output)
    {
    }

    @Override
    public String getOutputName()
    {
      return "dontCare";
    }

    @Override
    public String resolveOutputName() throws IOException
    {
      return "dontCare";
    }

    @Override
    public void moveOutput(String toName) throws IOException
    {
    }
  }

  public class NormalMapOp extends MapOp implements OutputProducer
  {
    private MapOpActionListener listener;
    private String id;
    private List<MapOp> inputs = new ArrayList<MapOp>();

    public NormalMapOp(String id, MapOpActionListener listener)
    {
      this.id = id;
      this.listener = listener;
    }

    @Override
    public void addInput(MapOp n) throws IllegalArgumentException
    {
      inputs.add(n);
    }

    @Override
    public List<MapOp> getInputs()
    {
      return inputs;
    }

    @Override
    public void build(Progress p) throws IOException, JobFailedException, JobCancelledException
    {
      listener.addAction(id, BUILD_ACTION);
    }

    @Override
    public void postBuild(Progress p, boolean buildPyramid) throws IOException, JobFailedException,
        JobCancelledException
    {
      listener.addAction(id, POST_BUILD_ACTION);
    }

    @Override
    public void setOutputName(String output)
    {
    }

    @Override
    public String getOutputName()
    {
      return "dontCare";
    }

    @Override
    public String resolveOutputName() throws IOException
    {
      return "dontCare";
    }

    @Override
    public void moveOutput(String toName) throws IOException
    {
    }
  }

  public class NormalMapOpNoOutput extends MapOp
  {
    private MapOpActionListener listener;
    private String id;
    private List<MapOp> inputs = new ArrayList<MapOp>();

    public NormalMapOpNoOutput(String id, MapOpActionListener listener)
    {
      this.id = id;
      this.listener = listener;
    }

    @Override
    public void addInput(MapOp n) throws IllegalArgumentException
    {
      inputs.add(n);
    }

    @Override
    public List<MapOp> getInputs()
    {
      return inputs;
    }

    @Override
    public void build(Progress p) throws IOException, JobFailedException, JobCancelledException
    {
      listener.addAction(id, BUILD_ACTION);
    }

    @Override
    public void postBuild(Progress p, boolean buildPyramid) throws IOException, JobFailedException,
        JobCancelledException
    {
      listener.addAction(id, POST_BUILD_ACTION);
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testSingleNoOutput() throws JobFailedException, JobCancelledException
  {
    int mapOpId = 1;
    MapOpActionListener listener = new MapOpActionListener();
    String rootId = "" + mapOpId;
    MapOp root = new NormalMapOpNoOutput(rootId, listener);
    executioner.setRoot(root);
    try
    {
      executioner.execute(conf, new ProgressHierarchy());
      Assert.fail("Expected a JobFailedException");
    }
    catch(JobFailedException e)
    {
      Assert.assertTrue(e.getMessage().equals("The last operation in the map algebra must produce output"));
    }
  }

  @Test
  @Category(UnitTest.class)
  public void testSingleDeferred() throws JobFailedException, JobCancelledException
  {
    int mapOpId = 1;
    MapOpActionListener listener = new MapOpActionListener();
    String rootId = "" + mapOpId;
    MapOp root = new DeferredMapOp1(rootId, listener);
    executioner.setRoot(root);
    executioner.execute(conf, new ProgressHierarchy());
    Assert.assertEquals(3, listener.size());
    Assert.assertEquals(rootId, listener.getActionId(0));
    Assert.assertEquals(PREPARE_ACTION, listener.getAction(0));
    Assert.assertEquals(rootId, listener.getActionId(1));
    Assert.assertEquals(BUILD_ACTION, listener.getAction(1));
    Assert.assertEquals(rootId, listener.getActionId(2));
    Assert.assertEquals(POST_BUILD_ACTION, listener.getAction(2));
  }

  @Test
  @Category(UnitTest.class)
  public void testSingleNormal() throws JobFailedException, JobCancelledException
  {
    int mapOpId = 1;
    MapOpActionListener listener = new MapOpActionListener();
    String rootId = "" + mapOpId;
    MapOp root = new NormalMapOp(rootId, listener);
    executioner.setRoot(root);
    executioner.execute(conf, new ProgressHierarchy());
    Assert.assertEquals(2, listener.size());
    Assert.assertEquals(rootId, listener.getActionId(0));
    Assert.assertEquals(BUILD_ACTION, listener.getAction(0));
    Assert.assertEquals(rootId, listener.getActionId(1));
    Assert.assertEquals(POST_BUILD_ACTION, listener.getAction(1));
  }

  @Test
  @Category(UnitTest.class)
  public void testNoChain1() throws JobFailedException, JobCancelledException
  {
    // Normal -> Deferred1
    int mapOpId = 1;
    MapOpActionListener listener = new MapOpActionListener();
    String rootId = "" + mapOpId++;
    String childId = "" + mapOpId;
    MapOp root = new NormalMapOp(rootId, listener);
    MapOp child = new DeferredMapOp1(childId, listener);
    root.addInput(child);
    executioner.setRoot(root);
    executioner.execute(conf, new ProgressHierarchy());
    Assert.assertEquals(4, listener.size());
    Assert.assertEquals(childId, listener.getActionId(0));
    Assert.assertEquals(PREPARE_ACTION, listener.getAction(0));
    Assert.assertEquals(childId, listener.getActionId(1));
    Assert.assertEquals(BUILD_ACTION, listener.getAction(1));
    // Root is last
    Assert.assertEquals(rootId, listener.getActionId(2));
    Assert.assertEquals(BUILD_ACTION, listener.getAction(2));
    Assert.assertEquals(rootId, listener.getActionId(3));
    Assert.assertEquals(POST_BUILD_ACTION, listener.getAction(3));
  }

  @Test
  @Category(UnitTest.class)
  public void testNoChain2() throws JobFailedException, JobCancelledException
  {
    // Deferred1 - > Normal
    int mapOpId = 1;
    MapOpActionListener listener = new MapOpActionListener();
    String rootId = "" + mapOpId++;
    String childId = "" + mapOpId;
    MapOp root = new DeferredMapOp1(rootId, listener);
    MapOp child = new NormalMapOp(childId, listener);
    root.addInput(child);
    executioner.setRoot(root);
    executioner.execute(conf, new ProgressHierarchy());
    Assert.assertEquals(4, listener.size());
    Assert.assertEquals(childId, listener.getActionId(0));
    Assert.assertEquals(BUILD_ACTION, listener.getAction(0));
    // Root is last
    Assert.assertEquals(rootId, listener.getActionId(1));
    Assert.assertEquals(PREPARE_ACTION, listener.getAction(1));
    Assert.assertEquals(rootId, listener.getActionId(2));
    Assert.assertEquals(BUILD_ACTION, listener.getAction(2));
    Assert.assertEquals(rootId, listener.getActionId(3));
    Assert.assertEquals(POST_BUILD_ACTION, listener.getAction(3));
  }

  @Test
  @Category(UnitTest.class)
  public void testNoChain3() throws JobFailedException, JobCancelledException
  {
    // Normal - > Normal
    int mapOpId = 1;
    MapOpActionListener listener = new MapOpActionListener();
    String rootId = "" + mapOpId++;
    String childId = "" + mapOpId;
    MapOp root = new NormalMapOp(rootId, listener);
    MapOp child = new NormalMapOp(childId, listener);
    root.addInput(child);
    executioner.setRoot(root);
    executioner.execute(conf, new ProgressHierarchy());
    Assert.assertEquals(3, listener.size());
    Assert.assertEquals(childId, listener.getActionId(0));
    Assert.assertEquals(BUILD_ACTION, listener.getAction(0));
    // Root is last
    Assert.assertEquals(rootId, listener.getActionId(1));
    Assert.assertEquals(BUILD_ACTION, listener.getAction(1));
    Assert.assertEquals(rootId, listener.getActionId(2));
    Assert.assertEquals(POST_BUILD_ACTION, listener.getAction(2));
  }

  @Test
  @Category(UnitTest.class)
  public void testNoChain4() throws JobFailedException, JobCancelledException
  {
    // Normal -> Deferred1 -> Deferred2
    int mapOpId = 1;
    MapOpActionListener listener = new MapOpActionListener();
    String rootId = "" + mapOpId++;
    String childId = "" + mapOpId++;
    String grandchildId = "" + mapOpId;
    MapOp root = new NormalMapOp(rootId, listener);
    MapOp child = new DeferredMapOp1(childId, listener);
    MapOp grandchild = new DeferredMapOp2(grandchildId, listener);
    root.addInput(child);
    child.addInput(grandchild);
    executioner.setRoot(root);
    executioner.execute(conf, new ProgressHierarchy());
    Assert.assertEquals(6, listener.size());
    int index = 0;
    Assert.assertEquals(grandchildId, listener.getActionId(index));
    Assert.assertEquals(PREPARE_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(grandchildId, listener.getActionId(index));
    Assert.assertEquals(BUILD_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(childId, listener.getActionId(index));
    Assert.assertEquals(PREPARE_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(childId, listener.getActionId(index));
    Assert.assertEquals(BUILD_ACTION, listener.getAction(index));
    index++;
    // Root is last
    Assert.assertEquals(rootId, listener.getActionId(index));
    Assert.assertEquals(BUILD_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(rootId, listener.getActionId(index));
    Assert.assertEquals(POST_BUILD_ACTION, listener.getAction(index));
  }

  @Test
  @Category(UnitTest.class)
  public void testNoChain5() throws JobFailedException, JobCancelledException
  {
    // Deferred1 -> Deferred2
    int mapOpId = 1;
    MapOpActionListener listener = new MapOpActionListener();
    String rootId = "" + mapOpId++;
    String childId = "" + mapOpId++;
    MapOp root = new DeferredMapOp1(rootId, listener);
    MapOp child = new DeferredMapOp2(childId, listener);
    root.addInput(child);
    executioner.setRoot(root);
    executioner.execute(conf, new ProgressHierarchy());
    Assert.assertEquals(5, listener.size());
    int index = 0;
    Assert.assertEquals(childId, listener.getActionId(index));
    Assert.assertEquals(PREPARE_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(childId, listener.getActionId(index));
    Assert.assertEquals(BUILD_ACTION, listener.getAction(index));
    index++;
    // Root is last
    Assert.assertEquals(rootId, listener.getActionId(index));
    Assert.assertEquals(PREPARE_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(rootId, listener.getActionId(index));
    Assert.assertEquals(BUILD_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(rootId, listener.getActionId(index));
    Assert.assertEquals(POST_BUILD_ACTION, listener.getAction(index));
  }

  @Test
  @Category(UnitTest.class)
  public void testChain1() throws JobFailedException, JobCancelledException
  {
    // Deferred1 -> Deferred1
    int mapOpId = 1;
    MapOpActionListener listener = new MapOpActionListener();
    String rootId = "" + mapOpId++;
    String childId = "" + mapOpId++;
    MapOp root = new DeferredMapOp1(rootId, listener);
    MapOp child = new DeferredMapOp1(childId, listener);
    root.addInput(child);
    executioner.setRoot(root);
    executioner.execute(conf, new ProgressHierarchy());
    Assert.assertEquals(4, listener.size());
    int index = 0;
    Assert.assertEquals(childId, listener.getActionId(index));
    Assert.assertEquals(PREPARE_ACTION, listener.getAction(index));
    index++;
    // Root is last
    Assert.assertEquals(rootId, listener.getActionId(index));
    Assert.assertEquals(PREPARE_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(rootId, listener.getActionId(index));
    Assert.assertEquals(BUILD_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(rootId, listener.getActionId(index));
    Assert.assertEquals(POST_BUILD_ACTION, listener.getAction(index));
  }

  @Test
  @Category(UnitTest.class)
  public void testChain2() throws JobFailedException, JobCancelledException
  {
    // Normal -> Deferred1 -> Deferred1
    int mapOpId = 1;
    MapOpActionListener listener = new MapOpActionListener();
    String rootId = "" + mapOpId++;
    String childId = "" + mapOpId++;
    String grandchildId = "" + mapOpId;
    MapOp root = new NormalMapOp(rootId, listener);
    MapOp child = new DeferredMapOp1(childId, listener);
    MapOp grandchild = new DeferredMapOp1(grandchildId, listener);
    root.addInput(child);
    child.addInput(grandchild);
    executioner.setRoot(root);
    executioner.execute(conf, new ProgressHierarchy());
    Assert.assertEquals(5, listener.size());
    int index = 0;
    Assert.assertEquals(grandchildId, listener.getActionId(index));
    Assert.assertEquals(PREPARE_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(childId, listener.getActionId(index));
    Assert.assertEquals(PREPARE_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(childId, listener.getActionId(index));
    Assert.assertEquals(BUILD_ACTION, listener.getAction(index));
    index++;
    // Root is last
    Assert.assertEquals(rootId, listener.getActionId(index));
    Assert.assertEquals(BUILD_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(rootId, listener.getActionId(index));
    Assert.assertEquals(POST_BUILD_ACTION, listener.getAction(index));
  }

  // Construct a map op tree with a parent and child map op that are both
  // DeferredExecution map ops. And assign an execute listener to the child.
  // The child should be prepared and built, followed by the execute listener
  // being built (but not prepared), and finally the root should be prepared,
  // built and then post-built.
  @Test
  @Category(UnitTest.class)
  public void testChainWithExecuteListener() throws JobFailedException, JobCancelledException
  {
    // Deferred1 -> Deferred1
    int mapOpId = 1;
    MapOpActionListener listener = new MapOpActionListener();
    String rootId = "" + mapOpId++;
    String childId = "" + mapOpId++;
    String listenerId = "" + mapOpId++;
    MapOp root = new DeferredMapOp1(rootId, listener);
    MapOp child = new DeferredMapOp1(childId, listener);
    MapOp executeListener = new NormalMapOp(listenerId, listener);
    child.addExecuteListener(executeListener);
    root.addInput(child);
    executioner.setRoot(root);
    executioner.execute(conf, new ProgressHierarchy());
    Assert.assertEquals(6, listener.size());
    int index = 0;
    Assert.assertEquals(childId, listener.getActionId(index));
    Assert.assertEquals(PREPARE_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(childId, listener.getActionId(index));
    Assert.assertEquals(BUILD_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(listenerId, listener.getActionId(index));
    Assert.assertEquals(BUILD_ACTION, listener.getAction(index));
    index++;
    // Root is last
    Assert.assertEquals(rootId, listener.getActionId(index));
    Assert.assertEquals(PREPARE_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(rootId, listener.getActionId(index));
    Assert.assertEquals(BUILD_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(rootId, listener.getActionId(index));
    Assert.assertEquals(POST_BUILD_ACTION, listener.getAction(index));
  }

  // Construct a map op tree with a parent and child map op that are both
  // DeferredExecution map ops. And assign an execute listener to the child.
  // The child should be prepared and built, followed by the execute listener
  // being built (but not prepared), and finally the root should be prepared,
  // built and then post-built.
  @Test
  @Category(UnitTest.class)
  public void testMultiChainWithExecuteListener() throws JobFailedException, JobCancelledException
  {
    // Deferred1 -> Deferred1
    int mapOpId = 1;
    MapOpActionListener listener = new MapOpActionListener();
    String rootId = "" + mapOpId++;
    String childId = "" + mapOpId++;
    String grandChildId = "" + mapOpId++;
    String listenerId = "" + mapOpId++;
    MapOp root = new DeferredMapOp1(rootId, listener);
    MapOp child = new DeferredMapOp1(childId, listener);
    MapOp grandChild = new DeferredMapOp1(grandChildId, listener);
    MapOp executeListener = new NormalMapOp(listenerId, listener);
    root.addInput(child);
    child.addInput(grandChild);
    child.addExecuteListener(executeListener);
    executioner.setRoot(root);
    executioner.execute(conf, new ProgressHierarchy());
    Assert.assertEquals(7, listener.size());
    int index = 0;
    Assert.assertEquals(grandChildId, listener.getActionId(index));
    Assert.assertEquals(PREPARE_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(childId, listener.getActionId(index));
    Assert.assertEquals(PREPARE_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(childId, listener.getActionId(index));
    Assert.assertEquals(BUILD_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(listenerId, listener.getActionId(index));
    Assert.assertEquals(BUILD_ACTION, listener.getAction(index));
    index++;
    // Root is last
    Assert.assertEquals(rootId, listener.getActionId(index));
    Assert.assertEquals(PREPARE_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(rootId, listener.getActionId(index));
    Assert.assertEquals(BUILD_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(rootId, listener.getActionId(index));
    Assert.assertEquals(POST_BUILD_ACTION, listener.getAction(index));
  }

  @Test
  @Category(UnitTest.class)
  public void testMultiChildren() throws JobFailedException, JobCancelledException
  {
    //                       Deferred1
    //                        /     \
    //                 Deferred2  Deferred1
    int mapOpId = 1;
    MapOpActionListener listener = new MapOpActionListener();
    String rootId = "" + mapOpId++;
    String childId1 = "" + mapOpId++;
    String childId2 = "" + mapOpId;
    MapOp root = new DeferredMapOp1(rootId, listener);
    MapOp child1 = new DeferredMapOp2(childId1, listener);
    MapOp child2 = new DeferredMapOp1(childId2, listener);
    root.addInput(child1);
    root.addInput(child2);
    executioner.setRoot(root);
    executioner.execute(conf, new ProgressHierarchy());
    Assert.assertEquals(6, listener.size());
    int index = 0;
    Assert.assertEquals(childId1, listener.getActionId(index));
    Assert.assertEquals(PREPARE_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(childId2, listener.getActionId(index));
    Assert.assertEquals(PREPARE_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(childId1, listener.getActionId(index));
    Assert.assertEquals(BUILD_ACTION, listener.getAction(index));
    index++;
    // Root is last
    Assert.assertEquals(rootId, listener.getActionId(index));
    Assert.assertEquals(PREPARE_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(rootId, listener.getActionId(index));
    Assert.assertEquals(BUILD_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(rootId, listener.getActionId(index));
    Assert.assertEquals(POST_BUILD_ACTION, listener.getAction(index));
  }

  @Test
  @Category(UnitTest.class)
  public void testMultiGeneration() throws JobFailedException, JobCancelledException
  {
    //                       Deferred1
    //                        /     \
    //                 Deferred2  Deferred1
    //                      /         \
    //               Deferred2      Deferred1
    //                                  \
    //                                 Normal
    int mapOpId = 1;
    MapOpActionListener listener = new MapOpActionListener();
    String rootId = "" + mapOpId++;
    String childId1 = "" + mapOpId++;
    String childId2 = "" + mapOpId++;
    String grandchildId11 = "" + mapOpId++;
    String grandchildId21 = "" + mapOpId++;
    String greatgrandchildId211 = "" + mapOpId++;
    MapOp root = new DeferredMapOp1(rootId, listener);
    MapOp child1 = new DeferredMapOp2(childId1, listener);
    MapOp child2 = new DeferredMapOp1(childId2, listener);
    MapOp grandchild11 = new DeferredMapOp2(grandchildId11, listener);
    MapOp grandchild21 = new DeferredMapOp1(grandchildId21, listener);
    MapOp greatgrandchild211 = new NormalMapOp(greatgrandchildId211, listener);
    root.addInput(child1);
    root.addInput(child2);
    child1.addInput(grandchild11);
    child2.addInput(grandchild21);
    grandchild21.addInput(greatgrandchild211);
    executioner.setRoot(root);
    executioner.execute(conf, new ProgressHierarchy());
    Assert.assertEquals(9, listener.size());
    int index = 0;
    Assert.assertEquals(grandchildId11, listener.getActionId(index));
    Assert.assertEquals(PREPARE_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(childId1, listener.getActionId(index));
    Assert.assertEquals(PREPARE_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(greatgrandchildId211, listener.getActionId(index));
    Assert.assertEquals(BUILD_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(grandchildId21, listener.getActionId(index));
    Assert.assertEquals(PREPARE_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(childId2, listener.getActionId(index));
    Assert.assertEquals(PREPARE_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(childId1, listener.getActionId(index));
    Assert.assertEquals(BUILD_ACTION, listener.getAction(index));
    index++;
    // Root is last
    Assert.assertEquals(rootId, listener.getActionId(index));
    Assert.assertEquals(PREPARE_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(rootId, listener.getActionId(index));
    Assert.assertEquals(BUILD_ACTION, listener.getAction(index));
    index++;
    Assert.assertEquals(rootId, listener.getActionId(index));
    Assert.assertEquals(POST_BUILD_ACTION, listener.getAction(index));
  }

  // TODO: cancel doesn't seem to do anything... Not sure how to test it
  @Ignore
  @Test
  @Category(UnitTest.class)
  public void cancel() throws FileNotFoundException, IOException, JobFailedException,
      InterruptedException, ExecutionException
  {
    executioner.executeChildren(conf, mapop, new ProgressHierarchy());
    
    //final ProgressHierarchy progress = new ProgressHierarchy();
    // need to run this in a thread?
    // executioner.execute(conf, progress);

    executioner.cancel();
  }

  @Test
  @Category(UnitTest.class)
  public void determineOutputs() throws FileNotFoundException, IOException, JobFailedException,
      InterruptedException, ExecutionException
  {
    executioner.executeChildren(conf, mapop, new ProgressHierarchy());

    // not sure how to test if this works, other then no exceptions...
  }

  @Test
  @Category(UnitTest.class)
  public void determineOutputsNoBounds() throws FileNotFoundException, IOException,
      JobFailedException, InterruptedException, ExecutionException
  {
    executioner.executeChildren(conf, mapop, new ProgressHierarchy());

    // not sure how to test if this works, other then no exceptions...
  }

  @Test
  //@Test(expected = NullPointerException.class)
  @Category(UnitTest.class)
  public void determineOutputsNoConf() throws FileNotFoundException, IOException,
      JobFailedException, InterruptedException, ExecutionException
  {
    executioner.executeChildren(null, mapop, new ProgressHierarchy());
  }

  @Test(expected = NullPointerException.class)
  @Category(UnitTest.class)
  public void determineOutputsNoMapOp() throws FileNotFoundException, IOException,
      JobFailedException, InterruptedException, ExecutionException
  {
    executioner.executeChildren(conf, null, new ProgressHierarchy());
  }

  @Test
  @Category(UnitTest.class)
  public void determineOutputsNoProgress() throws FileNotFoundException, IOException,
      JobFailedException, InterruptedException, ExecutionException
  {
    executioner.executeChildren(conf, mapop, null);

    // not sure how to test if this works, other then no exceptions...
  }

  @Ignore // ignored because I can't figure out a simple mapalgebra command only in core...
  @Test
  @Category(IntegrationTest.class)
  public void execute() throws IOException, JobFailedException, InterruptedException, ExecutionException, JobCancelledException
  {
    final ProgressHierarchy progress = new ProgressHierarchy();

    executioner.execute(conf, progress, true);
    
    String name = executioner.output;
    
    MrsImagePyramid pyramid = MrsImagePyramid.open(name, getProviderProperties());
    MrsImagePyramidMetadata metadata = pyramid.getMetadata();
    
    Assert.assertTrue("No pyramids built", pyramid.hasPyramids());
    
    for (int level = metadata.getMaxZoomLevel(); level >= 1; level--)
    {
      MrsImage image = pyramid.getImage(level);
      Assert.assertNotNull("MrsImage image missing for level " + level, image);
      image.close();
    }
    
    MrsImage image = pyramid.getHighestResImage();
    TestUtils.compareMrsImageToConstant(image, 2.0);
    image.close();

  }

  @Ignore // ignored because I can't figure out a simple mapalgebra command only in core...
  @Test
  @Category(IntegrationTest.class)
  public void executeNoPyramids() throws IOException, JobFailedException, InterruptedException, ExecutionException, JobCancelledException
  {
    final ProgressHierarchy progress = new ProgressHierarchy();

    executioner.execute(conf, progress);
    
    String name = executioner.output;
    
    MrsImagePyramid pyramid = MrsImagePyramid.open(name, getProviderProperties());    
    Assert.assertFalse("Pyramids built", pyramid.hasPyramids());
        
    MrsImage image = pyramid.getHighestResImage();
    TestUtils.compareMrsImageToConstant(image, 2.0);
    image.close();
  }

  // TODO:  Implement when preview is working...
  @Ignore
  @Test
  @Category(IntegrationTest.class)
  public void preview()
  {
    Assert.fail("Not yet implemented");
  }

  @Test
  @Category(UnitTest.class)
  public void setJobListener()
  {
    final JobListener jl = new JobListener(1234);
    executioner.setJobListener(jl);

    // nothing to do, just make sure the method doesn't except...
  }

  @Test
  @Category(UnitTest.class)
  public void setOutputPath()
  {
    final String path = "/foo/bar";
    executioner.setOutputName(path);

    Assert.assertEquals("Bad setOutputPath()", path, executioner.output);
  }

  @Test
  @Category(UnitTest.class)
  public void setOutputPath2()
  {
    final String path = "/foo/bar";
    executioner.setOutputName(path);

    Assert.assertEquals("Bad setOutputPath()", path, executioner.output);
  }


  @Test
  @Category(UnitTest.class)
  public void testSetRoot()
  {
    final MapOp root = new RasterMapOp()
    {
      @Override
      public void addInput(MapOp n) throws IllegalArgumentException
      {

      }

      @Override
      public void build(Progress p) throws IOException, JobFailedException, JobCancelledException
      {

      }
    };

    executioner.setRoot(root);

    Assert.assertSame("Not the same object!", root, executioner._root);
  }

  // TODO:  Not sure what this method does, so not sure how to test it...
  @Ignore
  @Test
  @Category(UnitTest.class)
  public void writeVectorOutput()
  {
    Assert.fail("Not yet implemented");
  }
}
