package com.vmware.vhadoop.vhm;

import java.util.List;

import com.vmware.vhadoop.external.VCActionDTOTypes.HostDTO;
import com.vmware.vhadoop.external.VCActionDTOTypes.VMDTO;

public class TTStatesForHost {
   private HostDTO _host;
   private List<VMDTO>_enabled;
   private List<VMDTO> _disabled;

   public TTStatesForHost(HostDTO host, List<VMDTO> enabled, List<VMDTO> disabled) {
      this._host = host;
      this._enabled = enabled;
      this._disabled = disabled;
   }
   public HostDTO getHost() {
      return _host;
   }
   public List<VMDTO> getEnabled() {
      return _enabled;
   }
   public List<VMDTO> getDisabled() {
      return _disabled;
   }
}
