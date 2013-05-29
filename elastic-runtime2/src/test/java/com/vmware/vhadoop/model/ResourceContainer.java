package com.vmware.vhadoop.model;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.vmware.vhadoop.model.allocation.Allocation;
import com.vmware.vhadoop.model.allocation.Allocator;


public abstract class ResourceContainer extends ResourceLimits implements Usage
{
   public static final long DEFAULT_SHARES = 4000;

   String id;
   long shares = DEFAULT_SHARES;
   long minMemory = Limits.UNLIMITED;
   long minCpu = Limits.UNLIMITED;
   protected Allocation allocation;

   protected Set<ResourceContainer> parents;
   Set<ResourceContainer> children;
   protected Map<Resource,Allocator> allocators;

   protected ResourceContainer(String id) {
      this.id = id;
      parents = new HashSet<ResourceContainer>();
      children = new HashSet<ResourceContainer>();
      allocators = new HashMap<Resource,Allocator>();
   }

   /**
    * Returns the resources that are available from this containers allocation to the
    * requesting child. Default implementation is that all resources are available to all children.
    * Passes the allocation along to parents to approve.
    *
    * @param child the child container making the request
    * @return the available resources
    */
   public Allocation getAvailableResources(ResourceContainer child) {
      Allocation available = allocation.clone();
      for (ResourceContainer container : parents) {
         available = available.minimums(container.getAvailableResources(this));
      }

      return available;
   }

   /**
    * Passes the request to the various allocators to process
    * @param request
    * @return
    */
   public Allocation allocate(String id, Allocation request) {
      Allocation actual = new Allocation(id);
      for (Allocator allocator : allocators.values()) {
         actual = actual.minimums(allocator.allocate(id, request));
      }

      return actual;
   }

   /**
    * Returns the memory in megabytes for all component allocations
    * @return
    */
   @Override
   public long getMemoryUsage() {
      long total = 0;
      for (ResourceContainer child : children) {
         total+= child.getMemoryUsage();
      }

      return total;
   }

   /**
    * Returns the active memory in megabytes for all component allocations
    * @return the active memory in Mb
    */
   @Override
   public long getActiveMemory() {
      long total = 0;
      for (ResourceContainer child : children) {
         total+= child.getActiveMemory();
      }

      return total;
   }

   /**
    * Returns the CPU allocation in Mhz for all component allocations
    * @return
    */
   @Override
   public long getCpuUsage() {
      long total = 0;
      for (ResourceContainer child : children) {
         total+= child.getCpuUsage();
      }

      return total;
   }


   /**
    * Gets the maximum stable interval for the container and it's children.
    * @return interval in milliseconds and/or instructions
    */
   public Interval getStableInterval() {
      Interval interval = new Interval();
      for (ResourceContainer child : children) {
         interval = interval.minimum(child.getStableInterval());
      }

      return interval;
   }

   /**
    * Adds a child container to this container
    * @param the child container
    */
   public void add(ResourceContainer container) {
      children.add(container);
      if (!container.parents.contains(this)) {
         container.addParent(this);
      }
   }

   /**
    * Removes the specified child from this container
    */
   public boolean remove(ResourceContainer child) {
      /* ensure symmetry, may already have been done */
      if (child.children.contains(this)) {
         child.removeParent(this);
      }

      return children.remove(child);
   }


   /**
    * Adds a parent container, to be notified when resource usage changes
    * @param parent - the container to notify
    */
   public void addParent(ResourceContainer parent) {
      parents.add(parent);
      /* ensure that we're added as a child so the relationship remains symmetrical */
      parent.add(this);
   }

   /**
    * Removes a parent container from the list
    * @param parent - the container to remove from the list
    */
   public void removeParent(ResourceContainer parent) {
      parents.remove(parent);
      /* ensure that we're removed as a child so the relationship remains symmetrical */
      parent.remove(this);
   }

   /**
    * Returns the id of the container
    * @return the string id of the container
    */
   public String getId() {
      return id;
   }

   /**
    * Finds the child entity with the given ID if it exists
    * @param id to look for
    * @return the child entity or null if not found
    */
   public ResourceContainer get(String id) {
      if (this.id.equals(id)) {
         return this;
      }

      for (ResourceContainer child : children) {
         ResourceContainer result = child.get(id);
         if (result != null) {
            return result;
         }
      }

      return null;
   }

   /**
    * Gets all entities of a given type.
    */
   public List<? extends ResourceContainer> get(Class<? extends ResourceContainer> type) {
      List<ResourceContainer> list = null;
      if (type.isAssignableFrom(this.getClass())) {
         if (list == null) {
            list = new LinkedList<ResourceContainer>();
         }
         list.add(this);
      }

      for (ResourceContainer child : children) {
         List<? extends ResourceContainer> result = child.get(type);
         if (result != null) {
            if (list == null) {
               /* can't just swap to the result due to the type casting */
               list = new LinkedList<ResourceContainer>();
            }
            list.addAll(result);
         }
      }

      return list;
   }

   @Override
   public String toString() {
      StringBuffer sb = new StringBuffer(id);
      sb.append(" cpu (Mhz): ").append(getCpuUsage());
      sb.append(" mem  (Mb): ").append(getMemoryUsage());

      return sb.toString();
   }

   /**
    * Constructs a basic string report about this container and its children
    * @param indent
    * @return
    */
   public String report(String indent) {
      final String newline = System.getProperty("line.separator");
      String indent2;
      StringBuffer sb;
      if (indent != null) {
         sb = new StringBuffer(indent);
         indent2 = indent + indent;
      } else {
         sb = new StringBuffer();
         indent = "";
         indent2 = "  ";
      }

      sb.append(toString());

      if (!children.isEmpty()) {
         for (ResourceContainer child : children) {
            sb.append(newline);
            sb.append(child.report(indent2));
         }
      } else {
         sb.append(newline);
      }

      return sb.toString();
   }

   public String report() {
      return report(null);
   }
}
