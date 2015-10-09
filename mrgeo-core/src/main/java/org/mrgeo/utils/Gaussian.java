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

public class Gaussian
{

// = 2.506628275;  Math.sqrt(2 * Math.PI);

// mean = 0, sigma = 1
public static double phi(double x)
{
  return Math.exp(-0.5 * x * x) / 2.506628275;
}

// mean = 0
public static double phi(double x, double sigma)
{
  double t = x / sigma;
  return Math.exp(-0.5 * t * t) / (2.506628275 * sigma);
}

public static double phi(double x, double mean, double sigma)
{
  double t = (x - mean) / sigma;
  return Math.exp(-0.5 * t * t) / (2.506628275 * sigma);
}

}
