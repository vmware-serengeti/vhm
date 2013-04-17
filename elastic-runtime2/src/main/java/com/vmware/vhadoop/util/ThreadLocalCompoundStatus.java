package com.vmware.vhadoop.util;

public class ThreadLocalCompoundStatus extends ThreadLocal<CompoundStatus> {
   class InitializedState {
      boolean _initialized;
   }
   
   ThreadLocal<InitializedState> _initializedState = new ThreadLocal<InitializedState>() {
      @Override
      public InitializedState get() {
         return new InitializedState();
      }
   };
   
   /* Ensures that only a single caller can initialize the ThreadLocal which is the same caller that removes it
    * This means that any code can call get() and if it's running in a thread that hasn't initialized a CompoundStatus
    *   the code gets a dummy CompoundStatus to avoid a NPE, but a memory leak cannot occur */
   public CompoundStatus initialize() {
      _initializedState.get()._initialized = true;
      return super.get();
   }
   
   @Override
   protected CompoundStatus initialValue() {
      return new CompoundStatus("TOP_LEVEL");
   }

   @Override
   public CompoundStatus get() {
      if (_initializedState.get()._initialized = false) {
         _initializedState.remove();
         return new CompoundStatus("DUMMY_STATUS");
      }
      return super.get();
   }
   
   @Override 
   public void remove() {
      super.remove();
      _initializedState.remove();
   }
}
