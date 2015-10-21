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

package org.mrgeo.utils

import scala.language.implicitConversions

object MrGeoImplicits {
  // value to Either<>
  implicit def left2Either[A,B](a:A):Either[A,B] = Left(a)
  implicit def right2Either[A,B](b:B):Either[A,B] = Right(b)

  // scala type to Number
  implicit def toNumber(x:Byte):java.lang.Number = new java.lang.Byte(x)
  implicit def toNumber(x:Int):java.lang.Number = new java.lang.Integer(x)
  implicit def toNumber(x:Short):java.lang.Number = new java.lang.Short(x)
  implicit def toNumber(x:Float):java.lang.Number = new java.lang.Float(x)
  implicit def toNumber(x:Double):java.lang.Number = new java.lang.Double(x)

  // java Number to scala type
  implicit def toByte(x:java.lang.Number):Byte = x.byteValue()
  implicit def toInt(x:java.lang.Number):Int = x.intValue()
  implicit def toShort(x:java.lang.Number):Short = x.shortValue()
  implicit def toFloat(x:java.lang.Number):Float = x.floatValue()
  implicit def toDouble(x:java.lang.Number):Double = x.doubleValue()

  // scala array to java Number array
  implicit def toNumber(x:Array[Byte]):Array[java.lang.Number] = x.map(v => new java.lang.Byte(v))
  implicit def toNumber(x:Array[Int]):Array[java.lang.Number] =  x.map(v => new java.lang.Integer(v))
  implicit def toNumber(x:Array[Short]):Array[java.lang.Number] =  x.map(v => new java.lang.Short(v))
  implicit def toNumber(x:Array[Float]):Array[java.lang.Number] =  x.map(v => new java.lang.Float(v))
  implicit def toNumber(x:Array[Double]):Array[java.lang.Number] =  x.map(v => new java.lang.Double(v))

  // java Number array to scala array
  implicit def toByte(x:Array[java.lang.Number]):Array[Byte] = x.map(v => v.byteValue())
  implicit def toInt(x:Array[java.lang.Number]):Array[Int] = x.map(v => v.intValue())
  implicit def toShort(x:Array[java.lang.Number]):Array[Short] = x.map(v => v.shortValue())
  implicit def toFloat(x:Array[java.lang.Number]):Array[Float] = x.map(v => v.floatValue())
  implicit def toDouble(x:Array[java.lang.Number]):Array[Double] = x.map(v => v.doubleValue())

}
