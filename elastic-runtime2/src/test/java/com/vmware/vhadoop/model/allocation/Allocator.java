package com.vmware.vhadoop.model.allocation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.vmware.vhadoop.model.Resource;

/**
 * Basic allocator shape. Tracks current allocations but performs no validation.
 * Subclasses should call the allocate/free methods on this base class unless they wish
 * to manage allocations themselves.
 * @author ghicken
 *
 */
public abstract class Allocator
{
   /** A list of the resource types managed by this allocator */
   Set<Resource> types;
   /** A list of the public subtypes maintained by this allocator */
   Set<String> subtypes;
   /** A list of the internal subtypes maintained by this allocator */
   Set<String> internalSubtypes;

   protected Allocation managed;
   Map<String,Allocation> allocations;

   /**
    * Creates an allocator for the resources owned by the specified container
    * @param container
    */
   protected Allocator(Allocation managedResources) {
      this.types = new HashSet<Resource>();
      this.subtypes = new HashSet<String>();
      this.internalSubtypes = new HashSet<String>();

      this.managed = managedResources;
      this.allocations = new HashMap<String,Allocation>();
   }

   /**
    * Used internally to set up the resources that the subclasses manages
    * @param managedResources
    */
   protected void setManagedResource(Resource type, String subtype, boolean internal, Long value) {
      types.add(type);
      if (internal) {
         subtypes.add(subtype);
      } else {
         internalSubtypes.add(subtype);
      }

      managed.setResourceUsage(type, subtype, value);
   }

   /**
    * Returns potentially allocatable resources
    * @return
    */
   public Allocation getAvailableResources() {
      return managed.clone();
   }

   /**
    * Creates an allocation. The returned allocation will be at least what was requested, or null if the
    * request could not be satisfied. There will likely be internal state returned as part of the allocation
    * that is opaque to the consuming workload. This clones the passed allocation so that we can be sure we've
    * got an unalterable record of what was actually allocated, even if the request is modified further.
    * The clone contains a link to the reference copy if we need it.
    *
    * @param id an id to associate with the allocation (used for corresponding free)
    * @param allocation the requested allocation
    * @return the actual allocation
    */
   public Allocation allocate(String id, Allocation allocation) {
      Allocation clone = allocation.clone();
      allocations.put(id, allocation);
      return clone;
   }

   /**
    * Frees an existing allocation for the given id
    * @return true if the allocation was found
    */
   public boolean free(Allocation allocation) {
      /* TODO: shift to stateless evaluation and weak references? */
      return allocations.remove(allocation.id) != null;
   }
}
