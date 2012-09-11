package com.vmware.vhadoop.external;

public interface GuestProcess<ExReturn, ExParam> {

   public interface ProcessDeathCallback {
      public void processDeath(int pid);
   }
   
   public void start(ProcessDeathCallback callback);

   public int getPID();

   public long getMemUsed();

   public boolean isHung();

   public boolean shutdown();

   public void kill();

   public String getName();

   public ExReturn execute(ExParam o);
}
