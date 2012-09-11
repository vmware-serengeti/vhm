package com.vmware.vhadoop.external;

/* New VHM can operate as a component of a VM, but is not in and of itself a VM - allows for embedding */
public abstract class VHMProcess implements GuestProcess<Object, Object> /* TODO: Too generic */ {

   private String _name;
   private long _memUsed;
   private boolean _modelInit;
   
   public void setModelInitParams(String name, long memUsed) {
      _name = name;
      _memUsed = memUsed;
      _modelInit = true;
   }

   @Override
   public void start(ProcessDeathCallback callback) {
      if (!_modelInit) {
         throw new RuntimeException("Model VHM not initialized");
      }
   }

   @Override
   public int getPID() {
      throw new RuntimeException("Not implemented");
   }

   @Override
   public long getMemUsed() {
      throw new RuntimeException("Not implemented");
   }

   @Override
   public boolean isHung() {
      throw new RuntimeException("Not implemented");
   }

   @Override
   public boolean shutdown() {
      throw new RuntimeException("Not implemented");
   }

   @Override
   public void kill() {
      throw new RuntimeException("Not implemented");
   }

   @Override
   public String getName() {
      throw new RuntimeException("Not implemented");
   }

   @Override
   public Object execute(Object o) {
      throw new RuntimeException("Not implemented");
   }

}
