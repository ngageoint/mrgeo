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
