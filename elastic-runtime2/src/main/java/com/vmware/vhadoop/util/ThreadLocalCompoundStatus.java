package com.vmware.vhadoop.util;

/* ThreadLocalCompoundStatus is a mechanism for stashing a Root CompoundStatus object in a thread
 * The idea is that any class wanting to add status to a thread's execution can do so if it has
 *   a reference to the ThreadLocalCompoundStatus. It can simply call get().addStatus(x)
 * The significant advantage to this is that CompoundStatuses do not have to be passed around through signatures
 *   and for re-entrant single instance classes, there is a guarantee that the right status will be updated
 * As further protection against memory leaks, the capability has been added for the ThreadLocalCompoundStatus
 *   to just return a dummy object unless the current thread explicitly called initialize(). That calling thread
 *   is then responsible for explicitly calling remove() once the call stack has returned. The other advantage
 *   of this is that code using the CompoundStatus does not need to be full of null checks. It will either get 
 *   a useless dummy or something useful.
 */
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
      return super.get();     /* Returns the result of initialValue() and then that same object, until remove() is called */
   }
   
   @Override
   protected CompoundStatus initialValue() {
      /* Return the root CompoundStatus for a thread operation. Any number of compound statuses can be added to this one */
      return new CompoundStatus("ROOT");
   }

   @Override
   public CompoundStatus get() {
      /* If this thread has not explicitly initialized its TLCS, it should not be allowed to create a ThreadLocal
       * Instead it gets a useless dummy */
      if (_initializedState.get()._initialized = false) {
         _initializedState.remove();
         return new CompoundStatus("DUMMY_STATUS");
      }
      return super.get();
   }
   
   @Override 
   public void remove() {
      /* Any thread that has called initialize(), must follow it up with a call to remove() once the operation is completed */
      super.remove();
      _initializedState.remove();
   }
}
