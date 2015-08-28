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

import org.mrgeo.mapalgebra.parser.ParserException;
import org.mrgeo.mapalgebra.parser.ParserFunctionNode;
import org.mrgeo.mapalgebra.parser.ParserNode;

import java.util.ArrayList;

/**
 * Classes that implement this interface use the ServiceLoader mechanism for
 * loading. Details can be found here:
 * http://download.oracle.com/javase/6/docs/api/java/util/ServiceLoader.html
 * 
 * The short version is: 
 *  1. Create a new MapOpFactory class 
 *  2. Create a META-INF/services/org.mrgeo.mapreduce.MapOpFactory file. This file
 *     should be included in the class path (e.g. JAR)
 *  3. Put a line in the new file with your fully qualified class name. 
 *     E.g. org.mrgeo.legion.mapalgebra.LegionMapOpFactory
 */
public interface MapOpFactoryHadoop
{
  public ArrayList<String> getMapOpNames();
  
  /**
   * Returns a new MapOp of the node is recognized by the factory, otherwise it returns null.
   * @param node
   * @return
   */
  public MapOpHadoop convertToMapOp(ParserFunctionNode node) throws ParserException;
  
  public MapOpHadoop convertToMapOp(ParserNode node) throws ParserException;
  
  /**
   * The root factory is used when converting child nodes in convertToMapOp.
   * @param rootFactory
   */
  public void setRootFactory(MapOpFactoryHadoop rootFactory);
}
