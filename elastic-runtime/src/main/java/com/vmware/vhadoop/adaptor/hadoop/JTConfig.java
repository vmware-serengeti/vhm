package com.vmware.vhadoop.adaptor.hadoop;

public class JTConfig {
   String _hadoopHomePath;
   String _excludeTTPath;

   public JTConfig(String hadoopHomePath, String excludeTTPath) {
      _hadoopHomePath = hadoopHomePath;
      _excludeTTPath = excludeTTPath;
   }
   
   public String getHadoopHomePath() {
      return _hadoopHomePath;
   }
   
   public String getExcludeTTPath() {
      return _excludeTTPath;
   }
}
