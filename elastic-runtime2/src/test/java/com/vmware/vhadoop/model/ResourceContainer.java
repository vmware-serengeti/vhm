package com.vmware.vhadoop.model;

import java.util.LinkedList;
import java.util.List;


public abstract class ResourceContainer extends ResourceLimits implements Usage
{
   public static final long DEFAULT_SHARES = 4000;

   String id;
   long shares = DEFAULT_SHARES;
   long maxMemory = Limits.UNLIMITED;
   long maxCpu = Limits.UNLIMITED;
   long minMemory = Limits.UNLIMITED;
   long minCpu = Limits.UNLIMITED;

   private List<ResourceContainer> parents;
   List<ResourceContainer> children;
   List<ResourceUsage> usages;
   Orchestrator orchestrator;

   ResourceContainer(Orchestrator orchestrator) {
      children = new LinkedList<ResourceContainer>();
      usages = new LinkedList<ResourceUsage>();
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
      maxMemory = allocation;
      if (maxMemory != Limits.UNLIMITED && getMemoryUsage() > maxMemory) {
         orchestrator.getAllocationPolicy().allocateMemory(maxMemory, children);
      }
   }

   /**
    * Sets the upper CPU utilization limit of the container
    * @param allocation - the limit on this container in Mhz, Limits.UNLIMITED means unlimited
    */
   @Override
   public void setCpuLimit(long allocation) {
      maxCpu = allocation;
      if (maxCpu != Limits.UNLIMITED && getCpuUsage() > maxCpu) {
         orchestrator.getAllocationPolicy().allocateCpu(maxCpu, children);
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
   void add(ResourceContainer container) {
      children.add(container);
      container.addParent(this);
   }

   /**
    * Adds a child usage to this container
    * @param the usage to add
    */
   void add(ResourceUsage usage) {
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
}
