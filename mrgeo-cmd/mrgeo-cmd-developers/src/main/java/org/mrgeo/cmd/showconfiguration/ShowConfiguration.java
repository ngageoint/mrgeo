package org.mrgeo.cmd.showconfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.mrgeo.cmd.Command;
import org.mrgeo.core.MrGeoConstants;
import org.mrgeo.core.MrGeoProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Properties;

/**
 * ShowConfiguration is a utility for showing the running configuration of
 * the environment.  This is meant to be a diagnostic of the environment.
 */
public class ShowConfiguration extends Command
{
  private FileSystem fs = null;
  private Properties props = null;
  
  //private Connector conn = null;
  
  @SuppressWarnings("unused")
  private static final Logger log = LoggerFactory.getLogger(ShowConfiguration.class);
  
    
  private void initialize(Configuration conf){
    try{
      fs = FileSystem.get(conf);
      //fs = HadoopFileUtils.getFileSystem();
    } catch(IOException ioe){
      System.out.println("Hadoop file system not available.");
    }
    props = MrGeoProperties.getInstance();
    
  } // end initialize
  
  
  public boolean isHadoopAvailable(){
    if(fs == null){
      return false;
    }
    return true;
  } // end isHadoopAvailable


  public String reportOS(){
    return "Operating system is " + System.getProperty("os.name");    
  } // end reportOS

  
  public String reportUser(){
    return "User is " + System.getProperty("user.name");
  } // end reportUser
  
  
  public String reportMrGeoSettingsProperties(){
    StringBuffer sb = new StringBuffer();
    Properties psp = new Properties();
    InputStream is = MrGeoProperties.class.getClass().getResourceAsStream(MrGeoConstants.MRGEO_SETTINGS);
    if(is == null){
      sb.append("MrGeo default file settings.properties does not exist.\n");
    } else {
      sb.append("Found default configuration file settings.properties.\n");
      try{
        psp.load(is);
        is.close();
      } catch(IOException ioe){
        sb.append("\tProblem loading settings.properties file.\n");
      }
      sb.append(reportProperties("\t", psp) + "\n");
    }
    return sb.toString();
  } // end getMrGeoSettingsProperties
  
  public boolean isMrGeoSettingsPropertiesAvailable(){
    InputStream is = MrGeoProperties.class.getClass().getResourceAsStream(MrGeoConstants.MRGEO_SETTINGS);
    if(is == null){
      return false;
    }
    return true;        
  } // end isMrGeoSettingsPropertiesAvailable
  
  public String reportMrGeoConfInfo(){
    StringBuffer sb = new StringBuffer();
    String mgh = System.getenv(MrGeoConstants.MRGEO_ENV_HOME);
    if(mgh == null){
      sb.append("MRGEO_HOME environment variable not set for the user running Hadoop.\n");
    } else {
      sb.append("MRGEO_HOME environment variable points to: " + mgh + "\n");
      if(!mgh.endsWith(File.separator)){
        mgh += File.separator;
      }
      File mghconf = new File(mgh + MrGeoConstants.MRGEO_CONF);
      if(!mghconf.exists()){
        sb.append("Local mrgeo.conf file does not exist.\n");
      } else {
        sb.append("Found mrgeo.conf at: " + mghconf.getAbsolutePath() + "\n");
        props = MrGeoProperties.getInstance();
        sb.append(reportProperties("\t", props));
      }
    }
    
    return sb.toString();
  } // end getMrGeoConfInfo
  
  
  public boolean isMrGeoConfAvailable(){
    String mgh = System.getenv(MrGeoConstants.MRGEO_ENV_HOME);
    if(mgh == null){
      return false;
    }
    if(!mgh.endsWith(File.separator)){
      mgh += File.separator;
    }
    File mghconf = new File(mgh + MrGeoConstants.MRGEO_CONF);

    return mghconf.exists();
  } // end isMrGeoConfAvailable

  public boolean existsInHDFS(String p){
    try{
      return fs.exists(new Path(p));
    } catch(IOException ioe){
      return false;
    }
    
  } // end existsInHDFS
  
  public String reportHDFSPath(String p){
    StringBuffer sb = new StringBuffer();
    
    if(props.get(p) == null){
      sb.append("HDFS directory for '" + p + "' is not set.\n");
    } else {
      sb.append("HDFS image directory: " + props.get(p));
      try{
        if(fs.exists(new Path((String)props.get(p)))){
          sb.append("\tExists: ");
          FileStatus fstat = fs.getFileStatus(new Path((String)props.get(p)));
          sb.append("\tuser.group = " + fstat.getOwner() + "." + fstat.getGroup());
          FsPermission fsperm = fstat.getPermission();
          sb.append("\tu: " + fsperm.getUserAction());         
          sb.append("\tg: " + fsperm.getGroupAction());            
          sb.append("\to: " + fsperm.getOtherAction() + "\n");            
        
        } else {
          sb.append("\tDoes not exist.\n");
        }
      } catch(IOException ioe){
        sb.append("\tDoes not exist.\n");
      }
    }
    return sb.toString();
  } // end showHDFSPath
  
  public String reportProperties(String pad, Properties p){
    StringBuffer sb = new StringBuffer();
    ArrayList<String> arrayKeys = new ArrayList<String>();
    Enumeration<Object> keys = p.keys();
    
    while(keys.hasMoreElements()){
      arrayKeys.add(keys.nextElement().toString());
    }

    Collections.sort(arrayKeys);
    
    for(String k : arrayKeys){
      String kstr = String.format("%45s", k);
      sb.append(pad + kstr + "\t= " + p.getProperty(k) + "\n");
    }
    
    return sb.toString();
  } // end showProperties
  
  public boolean isJobJarAvailable(){
    String jj = props.getProperty(MrGeoConstants.MRGEO_JAR);
    if(jj == null){
      return false;
    }
    File file = new File(jj);
    return file.exists();
    
  } // end isJobJarAvailable
  
  public String reportJobJar(){
    StringBuffer sb = new StringBuffer();
    String jj = props.getProperty(MrGeoConstants.MRGEO_JAR);
    sb.append("Jar for hadoop jobs ");
    if(jj == null){
      sb.append("is not assigned.\n");
      return sb.toString();
    }
    
    sb.append(jj + " ");
    if(isJobJarAvailable()){
      sb.append(" Exists!\n");
    } else {
      sb.append(" does not exist!\n");
    }
    return sb.toString();
  } // end reportJobJar
  
  
//  public boolean isAccumuloUsed(){
//    if(props.getProperty(MrGeoConstants.MRGEO_ACC_INST) != null &&
//        props.getProperty(MrGeoConstants.MRGEO_ACC_ZOO) != null &&
//        props.getProperty(MrGeoConstants.MRGEO_ACC_USER) != null &&
//        props.getProperty(MrGeoConstants.MRGEO_ACC_PASSWORD) != null){
//      return true;
//    }
//    return false;
//    
//  } // end isAccumuloUsed
  
//  public String reportAccumulo(){
//    StringBuffer sb = new StringBuffer();
//    sb.append("Looking at Accumulo:\n");
//    Instance inst = new ZooKeeperInstance(
//        props.getProperty(MrGeoConstants.MRGEO_ACC_INST),
//        props.getProperty(MrGeoConstants.MRGEO_ACC_ZOO)
//        );
//    
//    Connector conn = null;
//    try{
//      conn = inst.getConnector(
//          props.getProperty(MrGeoConstants.MRGEO_ACC_USER),
//          props.getProperty(MrGeoConstants.MRGEO_ACC_PASSWORD).getBytes()
//          );
//      sb.append("\tSuccess connecting to accumulo with u/p = " +
//          props.getProperty(MrGeoConstants.MRGEO_ACC_USER) + "/" +
//          props.getProperty(MrGeoConstants.MRGEO_ACC_PASSWORD) + "\n");
//      
//    } catch(AccumuloException ae){
//      sb.append("\tProblem connecting to Accumulo:\n");
//      sb.append(ae.getMessage());
//      
//    } catch(AccumuloSecurityException ase){
//      sb.append("\tProblem with security connecting to Accumulo:\n");
//      sb.append(ase.getMessage());
//    }   
//    
//    if(conn == null){
//      return sb.toString();
//    }
//    
//    // check on the table
//    if(props.getProperty(MrGeoConstants.MRGEO_ACC_TABLE) != null){
//      sb.append("Checking on table '" +
//          props.getProperty(MrGeoConstants.MRGEO_ACC_TABLE) +
//          "' -> ");
//      if(conn.tableOperations().exists(props.getProperty(MrGeoConstants.MRGEO_ACC_TABLE))){
//        sb.append("Exists!\n");
//      } else {
//        sb.append("Does not Exist!\n");
//      }
//      
//    }
////    SortedSet<String> tables = conn.tableOperations().list();
////    sb.append("Tables:\n");
////    
////    for(String t : tables){
////      sb.append("\t" + t + "\n");
////      try{
////        Iterable<Entry<String, String>> ent = conn.tableOperations().getProperties(t);
////        Iterator<Entry<String, String>> ei = ent.iterator();
////        while(ei.hasNext()){
////          Entry<String, String> e = ei.next();
////          sb.append("\t\t" + e.getKey() + " = " + e.getValue() + "\n");
////        }
////      } catch(AccumuloException ase){
////        
////      } catch(TableNotFoundException tnfe){
////        
////      }
////    }
//    
//    return sb.toString();
//  } // end reportAccumulo
  
  
//  public void xmlHDFSReport(String startEl, XMLStreamWriter xml) throws XMLStreamException{
//    if(props == null){
//      initialize();
//    }
//    if(props != null && fs != null){
//      
//      if(props.getProperty(MrGeoConstants.MRGEO_HDFS_COLORSCALE) != null){
//        xml.writeStartElement(startEl);
//        xmlHDFSReport2(props.getProperty(MrGeoConstants.MRGEO_HDFS_COLORSCALE), xml);
//        xml.writeEndElement();      
//      }
//      
//      if(props.getProperty(MrGeoConstants.MRGEO_HDFS_IMAGE) != null){
//        xml.writeStartElement(startEl);
//        xmlHDFSReport2(props.getProperty(MrGeoConstants.MRGEO_HDFS_IMAGE), xml);
//        xml.writeEndElement();      
//      }
//
//      if(props.getProperty(MrGeoConstants.MRGEO_HDFS_KML) != null){
//        xml.writeStartElement(startEl);
//        xmlHDFSReport2(props.getProperty(MrGeoConstants.MRGEO_HDFS_KML), xml);
//        xml.writeEndElement();      
//      }
//      
//      if(props.getProperty(MrGeoConstants.MRGEO_HDFS_RESOURCE) != null){
//        xml.writeStartElement(startEl);
//        xmlHDFSReport2(props.getProperty(MrGeoConstants.MRGEO_HDFS_RESOURCE), xml);
//        xml.writeEndElement();      
//      }
//      
//      if(props.getProperty(MrGeoConstants.MRGEO_HDFS_SHAPE) != null){
//        xml.writeStartElement(startEl);
//        xmlHDFSReport2(props.getProperty(MrGeoConstants.MRGEO_HDFS_SHAPE), xml);
//        xml.writeEndElement();      
//      }
//      
//      if(props.getProperty(MrGeoConstants.MRGEO_HDFS_TSV) != null){
//        xml.writeStartElement(startEl);
//        xmlHDFSReport2(props.getProperty(MrGeoConstants.MRGEO_HDFS_TSV), xml);
//        xml.writeEndElement();      
//      }
//      
//      if(props.getProperty(MrGeoConstants.MRGEO_HDFS_VECTOR) != null){
//        xml.writeStartElement(startEl);
//        xmlHDFSReport2(props.getProperty(MrGeoConstants.MRGEO_HDFS_VECTOR), xml);
//        xml.writeEndElement();      
//      }
//        
//    } else {
//      xml.writeStartElement(startEl);
//      if(props == null){
//        xml.writeAttribute("props", "null");
//      }
//      if(fs == null){
//        xml.writeAttribute("fs", "null");
//      }
//      xml.writeEndElement();      
//
//    }
//          
//  } // end xmlReport
//  
//  private void xmlHDFSReport2(String p, XMLStreamWriter xml) throws XMLStreamException{
//    xml.writeAttribute("fshome", fs.getHomeDirectory().toString());
//    xml.writeAttribute("name", p);
//    try{
//      if(fs.exists(new Path(p))){
//        FileStatus fstat;
//        fstat = fs.getFileStatus(new Path(p));
//        FsPermission fsperm = fstat.getPermission();
//        xml.writeAttribute("status", "exits");
//        xml.writeAttribute("user", fstat.getOwner());
//        xml.writeAttribute("group", fstat.getGroup());
//        xml.writeAttribute("u,g,w", fsperm.getUserAction() + "," + fsperm.getGroupAction() + "," + fsperm.getOtherAction());
//      } else {
//        xml.writeAttribute("status", "does not exist");
//        return;
//      }
//        
//    } catch(IOException ioe){
//      xml.writeAttribute("status", ioe.getMessage());
//    }
//        
//  } // end xmlReport
  
  
  public String buildReport(){
    StringBuffer sb = new StringBuffer();

    sb.append("Environment information for MrGeo configuration:" + "\n\n");
    
    // basics
    sb.append(reportOS() + "\n");
    sb.append(reportUser() + "\n");
    
    // check on JBoss settings
    if(isMrGeoSettingsPropertiesAvailable()){
      sb.append(reportMrGeoSettingsProperties());
    } else {
      sb.append(MrGeoConstants.MRGEO_SETTINGS + " is not available.\n");
    }
    
    // MrGeo info
    if(isMrGeoConfAvailable()){
      sb.append(reportMrGeoConfInfo() + "\n");
    }
    
    // HDFS checks
    if(props != null && fs != null){
      sb.append(reportHDFSPath(MrGeoConstants.MRGEO_HDFS_COLORSCALE));
      sb.append(reportHDFSPath(MrGeoConstants.MRGEO_HDFS_IMAGE));
      sb.append(reportHDFSPath(MrGeoConstants.MRGEO_HDFS_KML));
      sb.append(reportHDFSPath(MrGeoConstants.MRGEO_HDFS_RESOURCE));
      sb.append(reportHDFSPath(MrGeoConstants.MRGEO_HDFS_SHAPE));
      sb.append(reportHDFSPath(MrGeoConstants.MRGEO_HDFS_TSV));
      sb.append(reportHDFSPath(MrGeoConstants.MRGEO_HDFS_VECTOR));
    }

    // look for job jar
    sb.append(reportJobJar());
    sb.append("\n");
    
    // if Accumulo is in the configuration - check on Accumulo
//    if(isAccumuloUsed()){
//      sb.append(reportAccumulo());
//      
//    }
    
    return sb.toString();
  } // end buildReport
  
  
  @Override
  public int run(String[] args, Configuration conf, Properties providerProperties)
  {
    initialize(conf);
    System.out.println(buildReport());
    return 0;
  }
  
} // end ShowConfiguration
