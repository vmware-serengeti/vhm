package com.vmware.vhadoop.vhm;

import com.vmware.vhadoop.external.VCActionDTOTypes.HostDTO;
import com.vmware.vhadoop.external.VCActionDTOTypes.VMDTO;

public class TTStatesForHost {
   private HostDTO _host;
   private VMDTO[] _enabled;
   private VMDTO[] _disabled;

   public TTStatesForHost(HostDTO host, VMDTO[] enabled, VMDTO[] disabled) {
      this._host = host;
      this._enabled = enabled;
      this._disabled = disabled;
   }
   public HostDTO getHost() {
      return _host;
   }
   public VMDTO[] getEnabled() {
      return _enabled;
   }
   public VMDTO[] getDisabled() {
      return _disabled;
   }
}
