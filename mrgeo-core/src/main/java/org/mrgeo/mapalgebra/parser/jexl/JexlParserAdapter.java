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

package org.mrgeo.mapalgebra.parser.jexl;

import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlException;
import org.apache.commons.jexl2.MapContext;
import org.apache.commons.jexl2.Script;
import org.apache.commons.jexl2.parser.*;
import org.mrgeo.mapalgebra.parser.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    //engine.setSilent(false);
    //engine.setStrict(true);
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
    List<String> result = new ArrayList<String>();
    for (String functionName : twoArgFunctions.values())
    {
      result.add(functionName);
    }
    return result;
  }

  @Override
  public void addFunction(String functionName)
  {
  }

  @Override
  public ParserNode parse(String expression) throws ParserException
  {
    if (engine == null)
    {
      throw new ParserException("Map algebra parser engine was not initialized");
    }
    try
    {
      //@SuppressWarnings("unused")
      Script script = engine.createScript(expression);
      jexlRootNode = engine.getScript();
      //jexlRootNode = (ASTJexlScript)engine.createScript(expression);
      ParserNode last = null;
      for (int i = 0; i < jexlRootNode.jjtGetNumChildren(); i++)
      {
        last = convertToMrGeoNode(jexlRootNode.jjtGetChild(i));
//        if (factory != null)
//        {
//          factory.convertToMapOp(last);
//        }
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
        return -oldNum.intValue();
      }
      else if (oldNum instanceof Double)
      {
        return -oldNum.doubleValue();
      }
      else if (oldNum instanceof Float)
      {
        return new Float(0.0 - oldNum.floatValue());
      }
      else if (oldNum instanceof Short)
      {
        return -oldNum.shortValue();
      }
      else if (oldNum instanceof Long)
      {
        return -oldNum.longValue();
      }
      else if (oldNum instanceof Byte)
      {
        return -oldNum.byteValue();
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
      cn.setName(cn.getValue().toString());
      n = cn;
    }
    else if (node instanceof ASTStringLiteral)
    {
      ParserConstantNode cn = new ParserConstantNode();
      cn.setNativeNode(node);
      cn.setValue(((ASTStringLiteral) node).getLiteral());
      cn.setName(cn.getValue().toString());
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
        fn.setName(methodNameNode.image.toLowerCase());
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
    else
    {
      if (node instanceof ASTUnaryMinusNode)
      {
        // Handle negative numbers
        if (node.jjtGetNumChildren() == 1 && node.jjtGetChild(0) instanceof ASTNumberLiteral)
        {
          ParserConstantNode cn = new ParserConstantNode();
          cn.setNativeNode(node);

          cn.setValue(-(((ASTNumberLiteral) node.jjtGetChild(0)).getLiteral().doubleValue()));
          cn.setName(cn.getValue().toString());
          n = cn;

      parentNode = node;

        }
        else
        {
          ParserFunctionNode fn = new ParserFunctionNode();
          fn.setName("uminus");
          fn.setNativeNode(node);
          n = fn;
        }
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
          newParent.setName(((ASTAdditiveOperator) node.jjtGetChild(index)).image);
          newParent.addChild(previousOperand);
          newParent.addChild(convertToMrGeoNode(node.jjtGetChild(index + 1)));
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
    }
    if (n != null)
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
