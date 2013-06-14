package com.vmware.vhadoop.vhm.hadoop;

import com.vmware.vhadoop.vhm.hadoop.HadoopConnection.HadoopConnectionProperties;
import com.vmware.vhadoop.vhm.model.vcenter.VirtualCenter;

/**
 * Exists for the sole purpose of creating a model connection we can use for interception/injection
 */
public class ModelHadoopAdaptorThin extends HadoopAdaptor
{
   VirtualCenter vCenter;

   public ModelHadoopAdaptorThin(VirtualCenter vCenter) {
      super(new SimpleHadoopCredentials("vHadoopUser", "vHadoopPwd", "vHadoopPrvkeyFile"), new JTConfigInfo("vHadoopHome", "vHadoopExcludeTTFile"));
      this.vCenter = vCenter;
   }

   @Override
   protected HadoopConnection getHadoopConnection(HadoopClusterInfo cluster, HadoopConnectionProperties properties) {
      return new ModelHadoopConnection(vCenter, cluster, properties);
   }
}
