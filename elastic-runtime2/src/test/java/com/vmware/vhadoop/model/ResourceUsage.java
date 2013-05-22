package com.vmware.vhadoop.model;

import static com.vmware.vhadoop.model.Resource.CPU;
import static com.vmware.vhadoop.model.Resource.MEMORY;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

abstract public class ResourceUsage implements Usage
{
   String id;
   private Map<Resource,Object> usage;
   private List<ResourceContainer> parents;

   ResourceUsage(String id) {
      this.id = id;
      usage = new TreeMap<Resource,Object>();
      parents = new LinkedList<ResourceContainer>();
   }

   /**
    * Returns the allocated memory in megabytes
    * @return
    */
   @Override
   public long getMemoryUsage() {
      return (Long)(usage.get(MEMORY));
   }

   /**
    * Returns the CPU allocation in Mhz
    * @return
    */
   @Override
   public long getCpuUsage() {
      return (Long)(usage.get(CPU));
   }

   /**
    * Sets the allocated memory in megabytes
    * @param allocation
    */
   public void setMemoryUsage(long allocation) {
      usage.put(MEMORY, allocation);
      for (ResourceContainer parent : parents) {
         parent.update();
      }
   }

   /**
    * Sets the CPU allocation in Mhz
    * @param allocation
    */
   public void setCpuUsage(long allocation) {
      usage.put(CPU, allocation);
      for (ResourceContainer parent : parents) {
         parent.update();
      }
   }

   /**
    * Gets the maximum stable interval for the container and it's children. Should be overridden by
    * inheritors who don't have stable resource usage patterns.
    * @return Long.MAX_VALUE
    */
   @Override
   public long getStableInterval() {
      return Long.MAX_VALUE;
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
    * General toString description method
    * @return string description of usage
    */
   @Override
   public String toString() {
      return report(null);
   }

   /**
    * Model toString method that can take an indentation prefix
    * @param indent prefix
    * @return string description
    */
   public String report(String indent) {
      StringBuffer sb;
      if (indent != null) {
         sb = new StringBuffer(indent);
      } else {
         sb = new StringBuffer();
         indent = "  ";
      }

      sb.append(id);
      sb.append(indent).append(" cpu (Mhz): ").append(getCpuUsage());
      sb.append(indent).append(" mem  (Mb): ").append(getMemoryUsage());

      return sb.toString();
   }
}