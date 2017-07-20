package org.mrgeo.publisher;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.MrGeoProperties;
import org.mrgeo.image.MrsPyramidMetadata;
import org.mrgeo.junit.UnitTest;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mrgeo.publisher.MrGeoPublisherFactory.MRGEO_PUBLISHER_CLASS_PROP;
import static org.mrgeo.publisher.MrGeoPublisherFactory.MRGEO_PUBLISHER_CONFIGURATOR_CLASS_PROP;
import static org.mrgeo.publisher.PublisherProfileConfigProperties.MRGEO_PUBLISHER_PROPERTY_PREFIX;

@SuppressWarnings("all") // test code, not included in production
public class MrGeoPublisherFactoryTest
{
static final String TEST_PROPERTY = "testProperty";
static final String TEST_PROPERTY_VALUE = "testPropertyValue";
private static final String PROFILE1 = "profile1";
private static final String PROFILE2 = "profile2";
private static final String PROFILE1_PROP = "profile1Prop";
private static final String PROFILE2_PROP = "profile2Prop";
private static final String PROFILE1_PROP_VALUE = "profile1PropValue";
private static final String PROFILE2_PROP_VALUE = "profile2PropValue";
private MrGeoPublisher mockPublisher;
private MrGeoPublisherConfigurator mockConfigurator;
private Properties mrGeoProperties;


@Before
public void setUp() throws Exception
{
  MrGeoProperties.resetProperties();
  // Get the MrGeoProperties instance.  Because a static reference to the properties returned are held by the
  // MrGeoProperties class, we can inject any properties needed for testing
  mrGeoProperties = MrGeoProperties.getInstance();

  // remove any mrgeo publisher tags
  Set<String> remove = new HashSet<>();
  for (Object o : mrGeoProperties.keySet())
  {
    String key = (String) o;
    if (key.startsWith(MRGEO_PUBLISHER_PROPERTY_PREFIX))
    {
      remove.add(key);
    }
    ;
  }

  for (String key : remove)
  {
    mrGeoProperties.remove(key);
  }
}

@After
public void tearDown() throws Exception
{
  MrGeoProperties.resetProperties();

  MrGeoPublisherFactory.clearCache();
  MrGeoTestPublisherConfigurator1.clearArguments();
  MrGeoTestPublisherConfigurator2.clearArguments();
}

@Test
@Category(UnitTest.class)
public void testGetPublishers()
{
  // Basic test case
  mrGeoProperties.put(String.format(String.format(MRGEO_PUBLISHER_PROPERTY_PREFIX + ".%1$s." +
      MRGEO_PUBLISHER_CLASS_PROP, PROFILE1)), MrGeoTestPublisher1.class.getName());
  mrGeoProperties.put(String.format(MRGEO_PUBLISHER_PROPERTY_PREFIX + ".%1$s." +
      MRGEO_PUBLISHER_CONFIGURATOR_CLASS_PROP, PROFILE1), MrGeoTestPublisherConfigurator1.class.getName());
  mrGeoProperties.put(String.format(MRGEO_PUBLISHER_PROPERTY_PREFIX + ".%1$s." +
      PROFILE1_PROP, PROFILE1), PROFILE1_PROP_VALUE);
  // Get the list of publishers
  List<MrGeoPublisher> publishers = MrGeoPublisherFactory.getAllPublishers();
  assertEquals(1, publishers.size());
  assertTrue(publishers.get(0) instanceof MrGeoTestPublisher1);
  assertTrue(MrGeoTestPublisherConfigurator1.configurePublisherArguments.get(0) instanceof MrGeoTestPublisher1);
  assertTrue(MrGeoTestPublisherConfigurator1.configureProfilePropertiesArguments.get(0).stringPropertyNames()
      .contains(PROFILE1_PROP));
}

@Test
@Category(UnitTest.class)
public void testGetPublishersEmptyList()
{
  // Get the list of publishers
  List<MrGeoPublisher> publishers = MrGeoPublisherFactory.getAllPublishers();
  assertEquals(0, publishers.size());
}

@Test
@Category(UnitTest.class)
public void testGetPublisherMultipleProfiles()
{
  mrGeoProperties.put(String.format(String.format(MRGEO_PUBLISHER_PROPERTY_PREFIX + ".%1$s." +
      MRGEO_PUBLISHER_CLASS_PROP, PROFILE1)), MrGeoTestPublisher1.class.getName());
  mrGeoProperties.put(String.format(MRGEO_PUBLISHER_PROPERTY_PREFIX + ".%1$s." +
      MRGEO_PUBLISHER_CONFIGURATOR_CLASS_PROP, PROFILE1), MrGeoTestPublisherConfigurator1.class.getName());
  mrGeoProperties.put(String.format(MRGEO_PUBLISHER_PROPERTY_PREFIX + ".%1$s." +
      PROFILE1_PROP, PROFILE1), PROFILE1_PROP_VALUE);
  mrGeoProperties.put(String.format(String.format(MRGEO_PUBLISHER_PROPERTY_PREFIX + ".%1$s." +
      MRGEO_PUBLISHER_CLASS_PROP, PROFILE2)), MrGeoTestPublisher2.class.getName());
  mrGeoProperties.put(String.format(MRGEO_PUBLISHER_PROPERTY_PREFIX + ".%1$s." +
      MRGEO_PUBLISHER_CONFIGURATOR_CLASS_PROP, PROFILE2), MrGeoTestPublisherConfigurator2.class.getName());
  mrGeoProperties.put(String.format(MRGEO_PUBLISHER_PROPERTY_PREFIX + ".%1$s." +
      PROFILE2_PROP, PROFILE2), PROFILE2_PROP_VALUE);
  // Get the list of publishers
  List<MrGeoPublisher> publishers = MrGeoPublisherFactory.getAllPublishers();
  assertEquals(2, publishers.size());
  // Sort publishers by class name so we know what order they are in
  Collections.sort(publishers, new Comparator<MrGeoPublisher>()
  {
    @Override
    public int compare(MrGeoPublisher o1, MrGeoPublisher o2)
    {
      return o1.getClass().getName().compareTo(o2.getClass().getName());
    }
  });
  assertTrue(publishers.get(0) instanceof MrGeoTestPublisher1);
  assertTrue(MrGeoTestPublisherConfigurator1.configurePublisherArguments.get(0) instanceof MrGeoTestPublisher1);
  assertTrue(MrGeoTestPublisherConfigurator1.configureProfilePropertiesArguments.get(0).stringPropertyNames()
      .contains(PROFILE1_PROP));
  assertTrue(publishers.get(1) instanceof MrGeoTestPublisher2);
  assertTrue(MrGeoTestPublisherConfigurator2.configurePublisherArguments.get(0) instanceof MrGeoTestPublisher2);
  assertTrue(MrGeoTestPublisherConfigurator2.configureProfilePropertiesArguments.get(0).stringPropertyNames()
      .contains(PROFILE2_PROP));
}

@Test
@Category(UnitTest.class)
public void testGetPublisherMultipleProfilesSameConfigurator()
{
  mrGeoProperties.put(String.format(String.format(MRGEO_PUBLISHER_PROPERTY_PREFIX + ".%1$s." +
      MRGEO_PUBLISHER_CLASS_PROP, PROFILE1)), MrGeoTestPublisher1.class.getName());
  mrGeoProperties.put(String.format(MRGEO_PUBLISHER_PROPERTY_PREFIX + ".%1$s." +
      MRGEO_PUBLISHER_CONFIGURATOR_CLASS_PROP, PROFILE1), MrGeoTestPublisherConfigurator1.class.getName());
  mrGeoProperties.put(String.format(MRGEO_PUBLISHER_PROPERTY_PREFIX + ".%1$s." +
      PROFILE1_PROP, PROFILE1), PROFILE1_PROP_VALUE);
  mrGeoProperties.put(String.format(String.format(MRGEO_PUBLISHER_PROPERTY_PREFIX + ".%1$s." +
      MRGEO_PUBLISHER_CLASS_PROP, PROFILE2)), MrGeoTestPublisher2.class.getName());
  mrGeoProperties.put(String.format(MRGEO_PUBLISHER_PROPERTY_PREFIX + ".%1$s." +
      MRGEO_PUBLISHER_CONFIGURATOR_CLASS_PROP, PROFILE2), MrGeoTestPublisherConfigurator1.class.getName());
  mrGeoProperties.put(String.format(MRGEO_PUBLISHER_PROPERTY_PREFIX + ".%1$s." +
      PROFILE2_PROP, PROFILE2), PROFILE2_PROP_VALUE);
  // Get the list of publishers
  List<MrGeoPublisher> publishers = MrGeoPublisherFactory.getAllPublishers();
  assertEquals(2, publishers.size());
  // Sort publishers by class name so we know what order they are in
  Collections.sort(publishers, new Comparator<MrGeoPublisher>()
  {
    @Override
    public int compare(MrGeoPublisher o1, MrGeoPublisher o2)
    {
      return o1.getClass().getName().compareTo(o2.getClass().getName());
    }
  });
  MrGeoPublisher publisher = publishers.get(0);
  assertTrue(publisher instanceof MrGeoTestPublisher1);
  int index = MrGeoTestPublisherConfigurator1.configurePublisherArguments.indexOf(publisher);
  assertTrue(index >= 0);
  assertTrue(MrGeoTestPublisherConfigurator1.configurePublisherArguments.get(index) == publisher);
  assertTrue(MrGeoTestPublisherConfigurator1.configureProfilePropertiesArguments.get(index).stringPropertyNames()
      .contains(PROFILE1_PROP));
  publisher = publishers.get(1);
  assertTrue(publisher instanceof MrGeoTestPublisher2);
  index = MrGeoTestPublisherConfigurator1.configurePublisherArguments.indexOf(publisher);
  assertTrue(index >= 0);
  assertTrue(MrGeoTestPublisherConfigurator1.configurePublisherArguments.get(index) == publisher);
  assertTrue(MrGeoTestPublisherConfigurator1.configureProfilePropertiesArguments.get(index).stringPropertyNames()
      .contains(PROFILE2_PROP));

}

public static class MrGeoTestPublisher1 implements MrGeoPublisher
{
  @Override
  public void publishImage(String imageName, MrsPyramidMetadata imageMetadata) throws MrGeoPublisherException
  {

  }
}

public static class MrGeoTestPublisher2 implements MrGeoPublisher
{
  @Override
  public void publishImage(String imageName, MrsPyramidMetadata imageMetadata) throws MrGeoPublisherException
  {

  }
}

public static class MrGeoTestPublisherConfigurator1 implements MrGeoPublisherConfigurator
{
  // Hook to get the argument to configure after it's called
  static List<MrGeoPublisher> configurePublisherArguments = new ArrayList<>();
  static List<Properties> configureProfilePropertiesArguments = new ArrayList<>();

  static void clearArguments()
  {
    configurePublisherArguments.clear();
    configureProfilePropertiesArguments.clear();
  }

  @Override
  public void configure(MrGeoPublisher publisher, Properties profileProperties)
  {
    configurePublisherArguments.add(publisher);
    configureProfilePropertiesArguments.add(profileProperties);
  }
}

public static class MrGeoTestPublisherConfigurator2 implements MrGeoPublisherConfigurator
{
  // Hook to get the argument to configure after it's called
  static List<MrGeoPublisher> configurePublisherArguments = new ArrayList<>();
  static List<Properties> configureProfilePropertiesArguments = new ArrayList<>();

  static void clearArguments()
  {
    configurePublisherArguments.clear();
    configureProfilePropertiesArguments.clear();
  }

  @Override
  public void configure(MrGeoPublisher publisher, Properties profileProperties)
  {
    configurePublisherArguments.add(publisher);
    configureProfilePropertiesArguments.add(profileProperties);
  }
}

}
