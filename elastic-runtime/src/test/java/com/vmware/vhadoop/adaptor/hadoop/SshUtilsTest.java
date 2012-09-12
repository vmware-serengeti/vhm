package com.vmware.vhadoop.adaptor.hadoop;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.Test;

import com.jcraft.jsch.ChannelExec;
import com.vmware.vhadoop.adaptor.hadoop.HadoopConnection.SshUtils;
import com.vmware.vhadoop.adaptor.hadoop.HadoopErrorCodes.ParamTypes;
import com.vmware.vhadoop.util.LogFormatter;

public class SshUtilsTest {

   private static final Logger _log = Logger.getLogger(SshUtilsTest.class.getName());
   Map<ParamTypes, String> _paramValues = new HashMap<ParamTypes, String>();

   @Before
   public void init() {
      Logger.getLogger("").getHandlers()[0].setFormatter(new LogFormatter());
      _paramValues.put(ParamTypes.COMMAND, "recommission");
      _paramValues.put(ParamTypes.DRSCRIPT, "recommissionTTs.sh");
      _paramValues.put(ParamTypes.EXCLUDE_FILE, "/tmp/exclude.txt");
      _paramValues.put(ParamTypes.DRLIST, "rlist.txt");
      _paramValues.put(ParamTypes.HADOOP_HOME, "/tmp/hadoop");
      _paramValues.put(ParamTypes.JOBTRACKER, "10.141.20.40");
   }
   
//   @Test
   public void TestErrorLogValues() {
      HadoopErrorCodes hec = new HadoopErrorCodes();
      for (int i=0; i<201; i++) {
         hec.interpretErrorCode(_log, i, _paramValues);
      }
   }

//   @Test
   public void testSSH() {
     SshUtils test = new NonThreadSafeSshUtils();
     ChannelExec channel = test.createChannel(_log, new SimpleHadoopCredentials("hduser", "had00p"), "10.140.109.105", 22);
     ByteArrayOutputStream baos = new ByteArrayOutputStream();
     PrintStream ps = new PrintStream(baos);
     int retVal = test.exec(_log, channel, ps, "/tmp/wibble.sh");
     ps.flush();
     new HadoopErrorCodes().interpretErrorCode(_log, retVal, _paramValues);
     _log.log(Level.INFO, "Process output = \"" + baos.toString().trim() + "\"");
     test.cleanup(_log, ps, channel);
   }

   @Test
   public void testSCP() {
     SshUtils test = new NonThreadSafeSshUtils();
     ChannelExec channel = test.createChannel(_log, new SimpleHadoopCredentials("hduser", "had00p"), "10.140.109.105", 22);

     int retVal = test.scpBytes(_log, channel, "I love my bytes".getBytes(), "/opt/", "bentit.txt", "755");
     
     new HadoopErrorCodes().interpretErrorCode(_log, retVal, _paramValues);
     test.cleanup(_log, null, channel);
   }
}
