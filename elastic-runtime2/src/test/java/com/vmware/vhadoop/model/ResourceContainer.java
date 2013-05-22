package com.vmware.vhadoop.model;

import java.util.LinkedList;
import java.util.List;


public abstract class ResourceContainer extends ResourceLimits implements Usage
{
   public static final long DEFAULT_SHARES = 4000;

   String id;
   long shares = DEFAULT_SHARES;
   long minMemory = Limits.UNLIMITED;
   long minCpu = Limits.UNLIMITED;

   private List<ResourceContainer> parents;
   List<ResourceContainer> children;
   List<ResourceUsage> usages;
   protected Orchestrator orchestrator;

   protected ResourceContainer(String id) {
      this.id = id;
      parents = new LinkedList<ResourceContainer>();
      children = new LinkedList<ResourceContainer>();
      usages = new LinkedList<ResourceUsage>();
   }

   protected ResourceContainer(String id, Orchestrator orchestrator) {
      this(id);
      this.orchestrator = orchestrator;
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

      for (ResourceUsage usage : usages) {
         total+= usage.getMemoryUsage();
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

      for (ResourceUsage usage : usages) {
         total+= usage.getActiveMemory();
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

      for (ResourceUsage usage : usages) {
         total+= usage.getCpuUsage();
      }

      return total;
   }

   /**
    * Set the upper memory limit of the container
    * @param allocation - the limit on this container in megabytes, Limits.UNLIMITED means unlimted
    */
   @Override
   public void setMemoryLimit(long allocation) {
      super.setMemoryLimit(allocation);
      if (allocation != Limits.UNLIMITED && getMemoryUsage() > allocation) {
         orchestrator.getAllocationPolicy().allocateMemory(allocation, children);
      }
   }

   /**
    * Sets the upper CPU utilization limit of the container
    * @param allocation - the limit on this container in Mhz, Limits.UNLIMITED means unlimited
    */
   @Override
   public void setCpuLimit(long allocation) {
      super.setCpuLimit(allocation);
      if (allocation != Limits.UNLIMITED && getCpuUsage() > allocation) {
         orchestrator.getAllocationPolicy().allocateCpu(allocation, children);
      }
   }

   /**
    * Gets the maximum stable interval for the container and it's children.
    * @return interval in milliseconds
    */
   @Override
   public long getStableInterval() {
      long interval = Long.MAX_VALUE;
      for (ResourceContainer child : children) {
         long i = child.getStableInterval();
         if (i < interval) {
            interval = i;
         }
      }

      return interval;
   }

   /**
    * Adds a child container to this container
    * @param the child container
    */
   public void add(ResourceContainer container) {
      children.add(container);
      container.addParent(this);
   }

   /**
    * Adds a child usage to this container
    * @param the usage to add
    */
   public void add(ResourceUsage usage) {
      usages.add(usage);
      usage.addParent(this);
   }

   /**
    * Called by child containers or usages when resource usage changes
    */
   public void update() {

   }

   /**
    * Adds a parent container, to be notified when resource usage changes
    * @param parent - the container to notify
    */
   @Override
   public void addParent(ResourceContainer parent) {
      parents.add(parent);
   }

   /**
    * Removes a parent container from the list
    * @param parent - the container to remove from the list
    */
   @Override
   public void removeParent(ResourceContainer parent) {
      parents.remove(parent);
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

      if (!usages.isEmpty()) {
//         sb.append(indent2).append("Usages: ").append(newline);
         for (ResourceUsage usage : usages) {
            sb.append(newline);
            sb.append(usage.report(indent2));
         }
      }

      if (!children.isEmpty()) {
//         sb.append(indent2).append("Children: ").append(newline);
         for (ResourceContainer child : children) {
            sb.append(newline);
            sb.append(child.report(indent2));
         }
      }

      if (usages.isEmpty() && children.isEmpty()) {
         sb.append(newline);
      }

      return sb.toString();
   }

   public String report() {
      return report(null);
   }
}
