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

import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.JexlInfo;
import org.apache.commons.jexl2.parser.ASTJexlScript;

public class MrGeoJexlEngine extends JexlEngine
{
  private ASTJexlScript script;

  public MrGeoJexlEngine()
  {
  }

  @Override
  protected ASTJexlScript parse(CharSequence expression, JexlInfo info, Scope frame)
  {
    script = super.parse(expression, info, frame);
    return script;
  }

  public ASTJexlScript getScript()
  {
    return script;
  }
}
