/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
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

import org.apache.commons.lang.ClassUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.mrgeo.mapalgebra.parser.ParserAdapter;
import org.mrgeo.mapalgebra.parser.ParserAdapterFactory;
import org.mrgeo.mapalgebra.parser.ParserConstantNode;
import org.mrgeo.mapalgebra.parser.ParserException;
import org.mrgeo.mapalgebra.parser.ParserFunctionNode;
import org.mrgeo.mapalgebra.parser.ParserNode;
import org.mrgeo.mapalgebra.parser.ParserVariableNode;
import org.mrgeo.opimage.ConstantDescriptor;
import org.mrgeo.rasterops.OpImageRegistrar;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.adhoc.AdHocDataProvider;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * The map algebra parser parses a map algebra equation into a tree of nodes.
 * Each node is aware of its inputs (children) and produces either a raster
 * (RenderedOp) or vector (FeatureInputFormat) as output. Look at the
 * MapAlgebraExecutor for details on how the nodes are evaluated.
 */
public class MapAlgebraParser implements MapOpFactory
{
  private static final Logger log = LoggerFactory.getLogger(MapAlgebraParser.class);

  // maps from function name to factory
  private HashMap<String, MapOpFactory> _factoryMap;
  private HashSet<String> cachedOps = new HashSet<String>();
  private HashMap<String, MapOp> fileMap = new HashMap<String, MapOp>();
  private Pattern filePattern = Pattern.compile("\\s*\\[([^\\]]+)\\]\\s*");
  //this pattern returns file names in the expression without full paths only
//  private Pattern inputFileNamesPattern = Pattern.compile("(\\[\\w+\\.*\\w*\\])");

  private List<ResourceMapOpLoader> resourceLoaders = null;
  private HashMap<String, Class<? extends MapOp>> mapOps = new HashMap<String, Class<? extends MapOp>>();

  private HashMap<String, MapOp> _variables = new HashMap<String, MapOp>();

  private TreeMap<Integer, MapAlgebraPreprocessor> _preprocessors = null;
  private Configuration conf;
  private Properties providerProperties;
  private String protectionLevel;

  private ParserAdapter parser;

  public MapAlgebraParser()
  {
    init();
  }
  public MapAlgebraParser(final Configuration conf, final String protectionLevel,
      final Properties providerProps)
  {
    init();
    this.conf = conf;
    this.protectionLevel = protectionLevel;
    this.providerProperties = providerProps;
  }

  @SuppressWarnings("unchecked")
  private void _loadFactoryFunctions()
  {
    _factoryMap = new HashMap<String, MapOpFactory>();

    Reflections reflections = new Reflections("org.mrgeo");

    Set<Class<? extends MapOpFactory>> subTypes = 
        reflections.getSubTypesOf(MapOpFactory.class);

    for (Class<? extends MapOpFactory> clazz : subTypes)
    {
      if (clazz != this.getClass())
      {
        try
        {
          log.debug("Registering Factory: " + clazz.getCanonicalName());
          
          Object cl = clazz.newInstance();

          Method method;
          method = clazz.getMethod("getMapOpNames");
          Object o = method.invoke(cl);

          ArrayList<String> names;
          if (o != null)
          {
            names = (ArrayList<String>)o;

            for (String name: names)
            {
              log.debug("    " + name);
              parser.addFunction(name);
              ((MapOpFactory)cl).setRootFactory(this);
              _factoryMap.put(name, (MapOpFactory) cl);
            }
          }
        }
        catch (SecurityException e)
        {
          e.printStackTrace();
        }
        catch (NoSuchMethodException e)
        {
          e.printStackTrace();
        }
        catch (IllegalArgumentException e)
        {
          e.printStackTrace();
        }
        catch (IllegalAccessException e)
        {
          e.printStackTrace();
        }
        catch (InvocationTargetException e)
        {
          e.printStackTrace();
        }
        catch (InstantiationException e)
        {
          e.printStackTrace();
        }
      }
    }



//    ServiceLoader<MapOpFactory> loader = ServiceLoader
//        .load(org.mrgeo.mapreduce.MapOpFactory.class);
//
//    _factoryMap = new HashMap<String, org.mrgeo.mapreduce.MapOpFactory>();
//
//    for (MapOpFactory s : loader)
//    {
//      log.info("Found MapOpFactory: " + s.toString());
//      s.setRootFactory(this);
//      for (String n : s.getMapOpNames())
//      {
//        _factoryMap.put(n, s);
//        log.debug("Added MapOp: " + n + " to MapOpFactory: " + s.toString());
//        parser.addFunction(n, sum);
//      }
//    }
  }

  private List<ResourceMapOpLoader> getResourceLoaders()
  {
    if (resourceLoaders == null)
    {
      resourceLoaders = new ArrayList<ResourceMapOpLoader>();
      Reflections reflections = new Reflections(ClassUtils.getPackageName(ResourceMapOpLoader.class));

      final Set<Class<? extends ResourceMapOpLoader>> loaders = reflections
          .getSubTypesOf(ResourceMapOpLoader.class);
      for (Class<? extends ResourceMapOpLoader> loaderClass : loaders)
      {
        try
        {
          ResourceMapOpLoader loader = loaderClass.newInstance();
          resourceLoaders.add(loader);
        }
        catch (Exception e)
        {
          log.error("Exception in map algebra parser while instantiating resource loader " +
              loaderClass.getCanonicalName(), e);
        }
      }
    }
    return resourceLoaders;
  }

  private MapOp _loadResource(String file) throws ParserException
  {
    List<ResourceMapOpLoader> loaders = getResourceLoaders();
    for (ResourceMapOpLoader loader : loaders)
    {
      try
      {
        MapOp mapOp = loader.loadMapOpFromResource(file, providerProperties);
        if (mapOp != null)
        {
          return mapOp;
        }
      }
      catch (Exception e)
      {
        log.error("Exception in map algebra parser while opening resource " + file, e);
        throw new ParserException(e.getMessage());
      }
    }
//    MapOp result =  _loadVectorFile(file);
//    if (result != null)
//    {
//      return result;
//    }
//
//    result = _loadRasterFile(file);
//    if (result != null)
//    {
//      return result;
//    }

    try
    {
      // Check to see if the resource exists
      AdHocDataProvider dp = DataProviderFactory.getAdHocDataProvider(file,
          AccessMode.READ, providerProperties);
      if (dp != null)
      {
        ResourceMapOp pmo = new ResourceMapOp();
        pmo.setOutputName(file);
        return pmo;
      }
    }
    catch (IOException e)
    {
      throw new ParserException(String.format("Error opening %s. (%s)", file, e.toString()));
    }

    throw new ParserException(String.format(
        "The specified input image (%s) wasn't found.", file));

  }

//  private MapOp _loadRasterFile(String file) throws ParseException
//  {
//    RasterMapOpLoader loader = new RasterMapOpLoader();
//    return loader.loadMapOpFromResource(file);
//  }
//
//  private MapOp _loadVectorFile(String file) throws ParseException
//  {
//    MrsVectorMapOpLoader loader = new MrsVectorMapOpLoader();
//    MapOp mapOp = loader.loadMapOpFromResource(file);
//    if (mapOp != null)
//    {
//      return mapOp;
//    }
//    VectorFileMapOpLoader vfLoader = new VectorFileMapOpLoader();
//    try
//    {
//      return vfLoader.loadMapOpFromResource(file);
//    }
//    catch (IOException e)
//    {
//      log.error("Got exception while loading vector resource " + file, e);
//    }
//    return null;
//  }

  private static MapOp convertToMapOp(ParserConstantNode node)
  {
    RenderedImageMapOp result = new RenderedImageMapOp();
    result.setRenderedImageFactory(new ConstantDescriptor());
    // The ConstantDescriptor also takes a tilesize parameter, but we
    // don't know the value for tilesize, so RenderedImageMapOp will
    // append that value when it uses the factory to create the
    // ConstantOpImage.
    result.getParameters().add(Double.valueOf(node.getValue().toString()));

    return result;
  }

  @Override
  public MapOp convertToMapOp(ParserFunctionNode node) throws ParserException
  {
    log.debug(node.getName());
    MapOp result = null;

    Class<? extends MapOp> c = mapOps.get(node.getName());
    MapOpFactory f = _factoryMap.get(node.getName());

    if (c != null)
    {
      result = convertToMapOp(c, node);
    }
    else if (f != null)
    {
      result = f.convertToMapOp(node);
    }
    else if (node.getName().equals("="))
    {
      assert (node.getNumChildren() == 2);
      for (int i = 0; i < node.getNumChildren(); i++)
      {
        log.info("{}", node.getChild(i));
      }
      String var = ((ParserVariableNode) node.getChild(0)).getName();
      if (mapOps.containsKey(var))
      {
        throw new ParserException(
            String.format("Cannot use variable name %s because there is a a function of the same name", var));
      }
      result = convertToMapOp(node.getChild(1));
      _variables.put(var, result);
    }
    else
    {
      throw new ParserException(String.format(
          "The specified operation, '%s' - %s, is not supported.", node.getName(), node.getClass()
          .getName()));
    }

    return result;
  }

  private MapOp convertToMapOp(ParserVariableNode node) throws ParserException
  {
    log.debug(node.getName());

    MapOp result = null;

    if (fileMap.containsKey(node.getName()))
    {
      result = fileMap.get(node.getName());
    }
    else if (_variables.containsKey(node.getName()))
    {
      result = _variables.get(node.getName());
    }
    else
    {
      throw new ParserException("The specified variable is not valid. " + node.getName());
    }

    return result;
  }

  private MapOp convertToMapOp(Class<? extends MapOp> c, ParserFunctionNode node) throws ParserException
  {
    log.debug(node.getName());
    MapOp mo;
    try
    {
      mo = c.newInstance();
      mo.setFunctionName(node.getName());
    }
    catch (Exception e)
    {
      e.printStackTrace();
      throw new ParserException(String.format("Unable to instantiate %s", c.getName()));
    }

    Vector<ParserNode> children = new Vector<ParserNode>();
    for (int i = 0; i < node.getNumChildren(); i++)
    {
      children.add(node.getChild(i));
    }

    children = mo.processChildren(children, parser);

    for (ParserNode n : children)
    {
      MapOp child = convertToMapOp(n);
      child.setParent(mo);

      mo.addInput(child);
      // After each map is executed by RunnableMapOp, it will check to
      // see if there are execute listeners assigned to the map op, and
      // then execute those. The following code sets up each of the children
      // of a ProcedureMapOp as execute listeners so the ProcedureMapOp
      // will be executed.
      if (mo instanceof ProcedureMapOp)
      {
        child.addExecuteListener(mo);
      }
    }

    return mo;
  }

  @Override
  public MapOp convertToMapOp(ParserNode node) throws ParserException
  {
    // pad(level * 2);

    MapOp mapOp = null;
    if (node instanceof ParserFunctionNode)
    {
      mapOp = convertToMapOp((ParserFunctionNode) node);
    }
    if (node instanceof ParserConstantNode)
    {
      mapOp = convertToMapOp((ParserConstantNode) node);
    }
    if (node instanceof ParserVariableNode)
    {
      mapOp = convertToMapOp((ParserVariableNode) node);
    }
    if (mapOp != null)
    {
      if (mapOp.getProviderProperties() == null)
      {
        mapOp.setProviderProperties(providerProperties);
      }
      if (mapOp.getProtectionLevel() == null)
      {
        mapOp.setProtectionLevel(protectionLevel);
      }
      return mapOp;
    }

    throw new ParserException();
  }

//  private MapOp convertToMapOp(RenderedImageFactory desc, ASTFunNode node) throws ParseException
//  {
//    RenderedImageMapOp result = new RenderedImageMapOp();
//
//    result.setRenderedImageFactory(desc);
//
//    // Set the inputs to the operation. For each input, we also include a
//    // parameter for that input's NoData value. And finally, we include
//    // a parameter for the NoData value to use for the output.
//    for (int i = 0; i < node.jjtGetNumChildren(); i++)
//    {
//      MapOp childMapOp = convertToMapOp(node.jjtGetChild(i));
//      childMapOp.setParent(result);
//      result.addInput(childMapOp);
//    }
//
//    if (cachedOps.contains(node.getName()))
//    {
//      result.setUseCache(true);
//    }
//
//    return result;
//  }

  /**
   * An untested method.
   */
  @Override
  public ArrayList<String> getMapOpNames()
  {
    ArrayList<String> result = new ArrayList<String>();
    for (String name : parser.getFunctionNames())
    {
      result.add(name.toString());
    }
    return result;
  }

  private void init()
  {
    OpImageRegistrar.registerMrGeoOps();

    // include any computationally expensive operations in the cached ops list.
    // This will cause the tiles to be cached. It is also a good idea to add
    // operations that read from multiple sources to the cached list.
    cachedOps.add("slope");

    parser = ParserAdapterFactory.createParserAdapter();
    parser.initialize();


    // register mapops
    Reflections reflections = new Reflections("org.mrgeo");

    Set<Class<? extends MapOp>> subTypes = 
        reflections.getSubTypesOf(MapOp.class);

    log.debug("Registering MapOps:");
    for (Class<? extends MapOp> clazz : subTypes)
    {
      try
      {
        if (!Modifier.isAbstract(clazz.getModifiers()))
        {
          registerFunctions(clazz);
        }
      }
      catch (SecurityException e)
      {
        e.printStackTrace();
      }
      catch (NoSuchMethodException e)
      {
        e.printStackTrace();
      }
      catch (IllegalArgumentException e)
      {
        e.printStackTrace();
      }
      catch (IllegalAccessException e)
      {
        e.printStackTrace();
      }
      catch (InvocationTargetException e)
      {
        e.printStackTrace();
      }
    }

    for (String n : mapOps.keySet())
    {
      parser.addFunction(n);
    }

    _loadFactoryFunctions();

    parser.afterFunctionsLoaded();
  }

  private void registerFunctions(Class<? extends MapOp> clazz) throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException
  {
    Method m = clazz.getMethod("register");
    Object o = m.invoke(null);

    if (o != null)
    {
      String[] funcs = (String[]) o;
      if (funcs != null && funcs.length > 0)
      {
        for (String f : funcs)
        {
          if (f != null && f.length() > 0)
          {
            log.debug("  " + clazz.getCanonicalName() + " (" + f + ")");
            mapOps.put(f, clazz);
          }
        }
      }
    }
    else
    {
      String func = clazz.getSimpleName().replace("MapOp", "");
      mapOps.put(func, clazz);
    }
  }

  private String mapFilesToVariables(String expression) throws ParserException
  {
    Matcher m = filePattern.matcher(expression);
    HashMap<String, String> filesFound = new HashMap<String, String>();

    fileMap.clear();
    RenderedImageMapOp constMapOp = new RenderedImageMapOp();
    constMapOp.setRenderedImageFactory(new ConstantDescriptor());
    constMapOp.getParameters().add(Double.NaN);
    fileMap.put("NaN", constMapOp);

    int i = 0;
    while (m.find())
    {
      String file = m.group(1);
      if (!filesFound.containsKey(file))
      {
        String varName = String.format("__file_%d__", i);

        MapOp image = _loadResource(file);

        fileMap.put(varName, image);
        filesFound.put(varName, file);

        i++;
      }
    }

    String exp = expression;
    for (String varName : filesFound.keySet())
    {
      exp = exp.replace("[" + filesFound.get(varName) + "]", varName);
    }

    return exp;
  }

  private static String pad(int size)
  {
    String result = "";
    for (int i = 0; i < size; i++)
    {
      result = result + "  ";
    }
    return result;
  }

  public TreeMap<Integer, MapAlgebraPreprocessor> getPreprocessors()
  {
    if (_preprocessors == null)
    {
      ServiceLoader<MapAlgebraPreprocessor> loader = ServiceLoader
          .load(MapAlgebraPreprocessor.class);

      _preprocessors = new TreeMap<Integer, MapAlgebraPreprocessor>();

      for (MapAlgebraPreprocessor s : loader)
      {
        _preprocessors.put(s.getOrder(), s);
      }
    }

    return _preprocessors;
  }

  /*
   * Finds any file input parameters specified without full file paths and fills in their missing
   * paths by assuming them to reside in mrgeo.images
   */
//  private String convertFileNamesToFilePaths(String expression) throws FileNotFoundException, 
//  IOException,ParseException
//  {
//    String modifiedExp = expression;
//
//    Matcher matcher = inputFileNamesPattern.matcher(modifiedExp);
//    boolean result = matcher.find();
//    while (result) 
//    {
//      boolean found = false;
//      final String fileName = matcher.group().replaceFirst("\\[", "").replaceAll("\\]", "");
//      //loop through the search paths unit we find a file that exists
//      for (String s : _searchPath)
//      {
//        Path p;
//        if (!s.isEmpty())
//        {          
//          p = new Path(s, fileName);
//          FileSystem fs = HadoopFileUtils.getFileSystem(this.conf, p);
//          if (fs.exists(p)) {
//            found = true;
//            modifiedExp = 
//                modifiedExp.replaceFirst("\\[" + fileName + "\\]", "[" + s + fileName + "]");
//            log.debug("Expression: " + modifiedExp);
//            break;
//          }
//        }
//      }
//      if (!found) {
//        throw new ParseException(String.format(
//            "The specified input image (%s) wasn't found. Search Path: ", fileName, StringUtils.join(
//                _searchPath, ",")));        
//      }
//      result = matcher.find();
//    }
//    return modifiedExp;
//  }

  public MapOp parse(String expression) throws ParserException, FileNotFoundException, IOException
  {
    log.debug("Raw expression: " + expression);

    String exp = expression;

    _variables.clear();

    /*TreeMap<Integer, MapAlgebraPreprocessor> preprocessors = */getPreprocessors();

    // first break on any "\n"
    String[] lines = exp.split("\n");    
    // Remove all the comments.
    ArrayList<String> cleaned = new ArrayList<String>();
    for (String line : lines)
    {
      line = line.trim();
      if (!line.startsWith("#"))
      {
        // any comments embedded on this line, if so, ignore the rest of the line...
        int comment = line.indexOf('#');
        if (comment > 0)
        {
        line = line.substring(0, comment).trim();
        }
        
        // make sure the line ends with a ";"
//        if (!line.endsWith(";"))
//        {
//          line += ';';
//        }
        cleaned.add(line);
      }
    }
    exp = StringUtils.join(cleaned, " ");
    
//    exp = convertFileNamesToFilePaths(exp);

    log.debug("Cleaned expression: " + exp);

    exp = mapFilesToVariables(exp);
    
    log.debug("Expression w/ mapped variables: " + exp);
    
//    FunctionTable ft = parser.getFunctionTable();
//    for (Object o : ft.keySet())
//    {
//      System.out.println(o.toString() + ": " + ft.get(o).toString());
//    }
    MapOp root = null;
    try
    {
      ParserNode rootNode = parser.parse(exp, this);
      if (rootNode != null)
      {
        root = convertToMapOp(rootNode);
        root.setDefaultConfiguration(conf);
      }
    }
    catch (ParserException e)
    {
      log.warn(exp);
      throw e;
    }

    log.debug("MapOp tree: \n" + root.toString());

    return root;
  }

  public static String toString(MapOp op)
  {
    return toString(op, 0);
  }

  public static String toString(MapOp op, int depth)
  {
    String result = pad(depth);
    result += op.toString() + "\n";
    for (MapOp child : op.getInputs())
    {
      result += toString(child, depth + 1);
    }
    return result;
  }

  @Override
  public void setRootFactory(MapOpFactory rootFactory)
  {
    // We are the root factory. No-op.
  }
}
