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

package org.mrgeo.mapalgebra.parser.jexl;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlException;
import org.apache.commons.jexl2.MapContext;
import org.apache.commons.jexl2.Script;
import org.apache.commons.jexl2.parser.ASTAdditiveNode;
import org.apache.commons.jexl2.parser.ASTAdditiveOperator;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTArrayLiteral;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTDivNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTMethodNode;
import org.apache.commons.jexl2.parser.ASTMulNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.mrgeo.mapalgebra.MapOpFactory;
import org.mrgeo.mapalgebra.parser.ParserAdapter;
import org.mrgeo.mapalgebra.parser.ParserConstantNode;
import org.mrgeo.mapalgebra.parser.ParserException;
import org.mrgeo.mapalgebra.parser.ParserFunctionNode;
import org.mrgeo.mapalgebra.parser.ParserNode;
import org.mrgeo.mapalgebra.parser.ParserVariableNode;

public class JexlParserAdapter implements ParserAdapter
{
  private MrGeoJexlEngine engine;
  private JexlContext context;
  private ASTJexlScript jexlRootNode;

  private Map<Class<? extends JexlNode>, String> twoArgFunctions;

  public JexlParserAdapter()
  {
    twoArgFunctions = new HashMap<Class<? extends JexlNode>, String>();
    twoArgFunctions.put(ASTAssignment.class, "=");
    twoArgFunctions.put(ASTLTNode.class, "<");
    twoArgFunctions.put(ASTLENode.class, "<=");
    twoArgFunctions.put(ASTGTNode.class, ">");
    twoArgFunctions.put(ASTGENode.class, ">=");
    twoArgFunctions.put(ASTEQNode.class, "==");
    twoArgFunctions.put(ASTNENode.class, "!=");
    twoArgFunctions.put(ASTDivNode.class, "/");
    twoArgFunctions.put(ASTMulNode.class, "*");
    twoArgFunctions.put(ASTAndNode.class, "&&");
    twoArgFunctions.put(ASTOrNode.class, "||");
  }

  @Override
  public void initialize()
  {
    engine = new MrGeoJexlEngine();
    engine.setSilent(false);
    engine.setStrict(true);
    context = new MapContext();
  }

  @Override
  public void initializeForTesting()
  {
  }

  @Override
  public void afterFunctionsLoaded()
  {
  }

  @Override
  public List<String> getFunctionNames()
  {
    return null;
  }

  @Override
  public void addFunction(String functionName)
  {
  }

  @Override
  public ParserNode parse(String expression, MapOpFactory factory) throws ParserException
  {
    if (engine == null)
    {
      throw new ParserException("Map algebra parser engine was not initialized");
    }
    try
    {
      @SuppressWarnings("unused")
      Script script = engine.createScript(expression);
      jexlRootNode = engine.getScript();
      ParserNode last = null;
      for (int i = 0; i < jexlRootNode.jjtGetNumChildren(); i++)
      {
        last = convertToMrGeoNode(jexlRootNode.jjtGetChild(i));
        if (factory != null)
        {
          factory.convertToMapOp(last);
        }
      }
      return last;
    }
    catch(JexlException e)
    {
      throw new ParserException(e);
    }
  }

  @Override
  public Object evaluate(ParserNode node) throws ParserException
  {
    JexlNode nativeNode = (JexlNode)node.getNativeNode();
    if (nativeNode instanceof ASTAssignment)
    {
      JexlNode valueNode = nativeNode.jjtGetChild(1);
      return valueNode.jjtGetValue();
    }
    else if (nativeNode instanceof ASTNumberLiteral)
    {
      return ((ASTNumberLiteral)nativeNode).getLiteral();
    }
    else if (nativeNode instanceof ASTStringLiteral)
    {
      return ((ASTStringLiteral)nativeNode).getLiteral();
    }
    else if ((nativeNode instanceof ASTUnaryMinusNode) &&
        (nativeNode.jjtGetChild(0) instanceof ASTNumberLiteral))
    {
      Number oldNum = ((ASTNumberLiteral)nativeNode.jjtGetChild(0)).getLiteral();
      if (oldNum instanceof Integer)
      {
        return new Integer(0 - oldNum.intValue());
      }
      if (oldNum instanceof Double)
      {
        return new Double(0.0 - oldNum.doubleValue());
      }
      if (oldNum instanceof Float)
      {
        return new Float(0.0 - oldNum.floatValue());
      }
      if (oldNum instanceof Short)
      {
        return new Short((short)(0 - oldNum.shortValue()));
      }
      if (oldNum instanceof Long)
      {
        return new Long(0 - oldNum.longValue());
      }
      if (oldNum instanceof Byte)
      {
        return new Byte((byte)(0 - oldNum.byteValue()));
      }
    }
    return ((JexlNode)node.getNativeNode()).jjtGetValue();
  }

  private ParserNode convertToMrGeoNode(JexlNode node)
  {
    ParserNode n = null;
    JexlNode parentNode = node;
    if (node instanceof ASTNumberLiteral)
    {
      ParserConstantNode cn = new ParserConstantNode();
      cn.setNativeNode(node);
      cn.setValue(((ASTNumberLiteral) node).getLiteral());
      n = cn;
    }
    else if (node instanceof ASTStringLiteral)
    {
      ParserConstantNode cn = new ParserConstantNode();
      cn.setNativeNode(node);
      cn.setValue(((ASTStringLiteral) node).getLiteral());
      n = cn;
    }
//    else if (node instanceof ASTAssignment)
//    {
//      ParserFunctionNode fn = new ParserFunctionNode();
////      ParserVariableNode vn = new ParserVariableNode();
//      // In some testing scenarios, the getVar() returns null.
////      JexlNode varNode = ((ASTAssignment) node).jjtGetChild(0);
////      vn.setName(varNode.image);
////      n = vn;
//      fn.setNativeNode(node);
//      fn.setName("=");
//      n = fn;
//    }
    else if (node instanceof ASTMethodNode)
    {
      // For method nodes, the actual method name is stored in
      // the first child node,
      JexlNode methodNameNode = node.jjtGetChild(0);
      if (methodNameNode instanceof ASTIdentifier)
      {
        ParserFunctionNode fn = new ParserFunctionNode();
        fn.setNativeNode(node);
        fn.setName(methodNameNode.image);
        n = fn;
        parentNode = null;
        // We have to process the children of a function call node
        // separately from other nodes because the first child of the
        // function call node is the name of the function, and we don't
        // want that child included as part of the children we return.
        for (int i=1; i < node.jjtGetNumChildren(); i++)
        {
          n.addChild(convertToMrGeoNode(node.jjtGetChild(i)));
        }
        return n;
      }
    }
    else if (node instanceof ASTUnaryMinusNode)
    {
      // Handle negative numbers
      ParserFunctionNode fn = new ParserFunctionNode();
      fn.setName("UMinus");
      fn.setNativeNode(node);
      n = fn;
//      parentNode = node;
    }
    else if (node instanceof ASTIdentifier)
    {
      ParserVariableNode vn = new ParserVariableNode();
      vn.setNativeNode(node);
      vn.setName(node.image);
      n = vn;
//      parentNode = node;
    }
    else if (node instanceof ASTNotNode)
    {
      ParserFunctionNode fn = new ParserFunctionNode();
      fn.setNativeNode(node);
      fn.setName("!");
      n = fn;
    }
    else if (node instanceof ASTArrayLiteral)
    {
      // This is invoked in the case where an image is specified in
      // map algebra (e.g. [myImage]). We translate this to the same
      // node structure that JEP used to generate.
      ParserFunctionNode fn = new ParserFunctionNode();
      fn.setName("LIST");
      fn.setNativeNode(node);
      n = fn;
      parentNode = node;
    }
    else if (node instanceof ASTAdditiveNode)
    {
      // This is used for two or more expressions being added/subtracted
      ParserNode newParent = null;
      int index = 1;
      ParserNode previousOperand = convertToMrGeoNode(node.jjtGetChild(0));
      while (index < node.jjtGetNumChildren())
      {
        newParent = new ParserFunctionNode();
        newParent.setName(((ASTAdditiveOperator)node.jjtGetChild(index)).image);
        newParent.addChild(previousOperand);
        newParent.addChild(convertToMrGeoNode(node.jjtGetChild(index+1)));
        previousOperand = newParent;
        index += 2;
      }
      return newParent;

//      n.addChild(convertToMrGeoNode(node.jjtGetChild(0)));
//      ParserFunctionNode fn = new ParserFunctionNode();
//      JexlNode operatorNode = node.jjtGetChild(1);
//      fn.setName(operatorNode.image);
//      n = fn;
//      parentNode = null;
//      // We need to process all but the second child, so we have to do that
//      // with special purpose code here.
//      n.addChild(convertToMrGeoNode(node.jjtGetChild(0)));
//      n.addChild(convertToMrGeoNode(node.jjtGetChild(2)));
//      return n;
    }
//    else if (node instanceof ASTDivNode)
//    {
//      ParserFunctionNode fn = new ParserFunctionNode();
//      fn.setName("/");
//      fn.setNativeNode(node);
//      n = fn;
//      parentNode = node;
//    }
//    else if (node instanceof ASTMulNode)
//    {
//      ParserFunctionNode fn = new ParserFunctionNode();
//      fn.setName("*");
//      fn.setNativeNode(node);
//      n = fn;
//      parentNode = node;
//    }
    else if (twoArgFunctions.containsKey(node.getClass()))
    {
      ParserFunctionNode fn = new ParserFunctionNode();
      fn.setName(twoArgFunctions.get(node.getClass()));
      fn.setNativeNode(node);
      n = fn;
      parentNode = node;
    }
    else if ((node instanceof ASTReference) || (node instanceof ASTReferenceExpression))
    {
      if (node.jjtGetNumChildren() > 0)
      {
        JexlNode child = node.jjtGetChild(0);
        return convertToMrGeoNode(child);
      }
    }
    if (n != null && parentNode != null)
    {
      for (int i=0; i < parentNode.jjtGetNumChildren(); i++)
      {
        n.addChild(convertToMrGeoNode(parentNode.jjtGetChild(i)));
      }
      return n;
    }
    return null;
  }
}
