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

package org.mrgeo.utils;

import java.lang.reflect.Array;

public final class HashCodeUtil {

/**
 * An initial value for a <tt>hashCode</tt>, to which is added contributions
 * from fields. Using a non-zero value decreases collisons of <tt>hashCode</tt>
 * values.
 */
public static final int SEED = 23;

/** booleans.  */
public static int hash(int aSeed, boolean aBoolean) {
  return firstTerm( aSeed ) + (aBoolean ? 1 : 0);
}

/*** chars.  */
public static int hash(int aSeed, char aChar) {
  return firstTerm(aSeed) + (int)aChar;
}

/** ints.  */
public static int hash(int aSeed , int aInt) {
    /*
    * Implementation Note
    * Note that byte and short are handled by this method, through
    * implicit conversion.
    */
  return firstTerm(aSeed) + aInt;
}

/** longs.  */
public static int hash(int aSeed , long aLong) {
  return firstTerm(aSeed)  + (int)(aLong ^ (aLong >>> 32));
}

/** floats.  */
public static int hash(int aSeed , float aFloat) {
  return hash(aSeed, Float.floatToIntBits(aFloat));
}

/** doubles. */
public static int hash(int aSeed , double aDouble) {
  return hash( aSeed, Double.doubleToLongBits(aDouble) );
}

/**
 * <tt>aObject</tt> is a possibly-null object field, and possibly an array.
 *
 * If <tt>aObject</tt> is an array, then each element may be a primitive
 * or a possibly-null object.
 */
public static int hash(int aSeed , Object aObject) {
  int result = aSeed;
  if (aObject == null){
    result = hash(result, 0);
  }
  else if (!isArray(aObject)){
    result = hash(result, aObject.hashCode());
  }
  else {
    int length = Array.getLength(aObject);
    for (int idx = 0; idx < length; ++idx) {
      Object item = Array.get(aObject, idx);
      //if an item in the array references the array itself, prevent infinite looping
      if(! (item == aObject))
        //recursive call!
        result = hash(result, item);
    }
  }
  return result;
}

// PRIVATE
private static final int fODD_PRIME_NUMBER = 37;

private static int firstTerm(int aSeed){
  return fODD_PRIME_NUMBER * aSeed;
}

private static boolean isArray(Object aObject){
  return aObject.getClass().isArray();
}
}
