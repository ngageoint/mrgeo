package org.mrgeo.rasterops;

import java.util.HashMap;

/**
 * A very simple options parser to replace the usage of commons cli in places where we need 
 * thread safety. It assumes that "args" contains a list of Strings of the following form:
 *    args = new String[]{"-arg1", "val1", "-arg2", "val2", ...}
 *  
 * Note the hyphen in front of every "arg" - that is mandatory.
 * 
 */
public class SimpleOptionsParser
{
  private HashMap<String,String> options;
  
  public SimpleOptionsParser(String[] args) {
    if(args.length % 2 != 0)
      throw new IllegalArgumentException("Expecting an even number of arguments");
    
    options = new HashMap<String,String>();
    for(int i=0; i < args.length; i+=2) {
      if(i % 2 == 0) {
        if(!args[i].startsWith("-")) {
          throw new IllegalArgumentException(
                      String.format("Argument \"%s\" does not start with " + 
                                    "\"-\". Every argument should start with \"-\""));
        }
        String argStripped = args[i].substring(1);
        options.put(argStripped, args[i+1]);
        
      }
    }
  }
  
  public String getOptionValue(String opt) {
    return options.get(opt);
  }
  
  public boolean isOptionProvided(String opt) {
    return options.containsKey(opt);
  }
}
