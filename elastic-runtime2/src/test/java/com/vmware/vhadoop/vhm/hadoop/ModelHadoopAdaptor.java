package com.vmware.vhadoop.vhm.hadoop;

import com.vmware.vhadoop.util.ThreadLocalCompoundStatus;
import com.vmware.vhadoop.vhm.hadoop.HadoopConnection.HadoopConnectionProperties;
import com.vmware.vhadoop.vhm.model.vcenter.VirtualCenter;

/**
 * Exists for the sole purpose of creating a model connection we can use for interception/injection
 */
public class ModelHadoopAdaptor extends HadoopAdaptor
{
   VirtualCenter vCenter;

   public ModelHadoopAdaptor(VirtualCenter vCenter, ThreadLocalCompoundStatus tlcs) {
      super(new SimpleHadoopCredentials("vHadoopUser", "vHadoopPwd", "vHadoopPrvkeyFile"), new JTConfigInfo("vHadoopHome", "vHadoopExcludeTTFile"), tlcs);
      this.vCenter = vCenter;
   }

   @Override
   protected HadoopConnection getHadoopConnection(HadoopClusterInfo cluster, HadoopConnectionProperties properties) {
      return new ModelHadoopConnection(vCenter, cluster, properties);
   }
}
