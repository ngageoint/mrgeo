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

package org.mrgeo.mapalgebra

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.mrgeo.mapalgebra.parser.ParserNode

@SuppressFBWarnings(value = Array("NM_CLASS_NAMING_CONVENTION"), justification = "Well, yes it does!")
trait MapOpRegistrar extends Externalizable {
  def register:Array[String]

  // apply should call the mapop constructor, which needs to throw ParserExceptions on errors
  def apply(node:ParserNode, variables:String => Option[ParserNode]):MapOp

  override def toString:String = getClass.getSimpleName.replace("$", "")

  // no ops. This prevents accidental serialization
  override def readExternal(in:ObjectInput):Unit = {}

  override def writeExternal(out:ObjectOutput):Unit = {}
}
