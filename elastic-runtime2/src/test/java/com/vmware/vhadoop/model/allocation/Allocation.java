package com.vmware.vhadoop.model.allocation;

import java.util.Map;
import java.util.TreeMap;

import com.vmware.vhadoop.model.Resource;

/**
 * General purpose resource allocation tracking. Allocators of various types store their data in here.
 * @author ghicken
 *
 */
public class Allocation
{
   String id;
   Allocation referenceCopy;
   Map<Resource,Map<String,Long>> resources;

   /**
    * Creates a resource allocation request with the specified id
    * @param id
    */
   public Allocation(String id) {
      this.id = id;
      resources = new TreeMap<Resource,Map<String,Long>>();
   }

   /**
    * Package copy constructor
    * @param allocation
    */
   Allocation(Allocation allocation) {
      this(allocation.id);

      for (Resource resource : resources.keySet()) {
         Map<String,Long> values = resources.get(resource);
         for (String subtype : values.keySet()) {
            this.setResourceUsage(resource, subtype, values.get(subtype));
         }
      }

      referenceCopy = allocation;
   }

   @Override
   public Allocation clone() {
      return new Allocation(this);
   }

   public long setResourceUsage(Resource type, String subtype, Long value) {
      Map<String,Long> resource = resources.get(type);
      if (resource == null) {
         resource = new TreeMap<String,Long>();
         resources.put(type, resource);
      }

      Long previous = resource.put(subtype, value);
      if (previous == null) {
         return 0;
      }

      return previous;
   }

   public long getResourceUsage(Resource type, String subtype) {
      Map<String,Long> resource = resources.get(type);
      if (resource == null) {
         return 0;
      }

      Long value = resource.get(subtype);
      if (value == null) {
         return 0;
      }

      return value;
   }

   public Allocation minimums(Allocation allocation) {
      Allocation minimum = clone();

      for (Resource resource : allocation.resources.keySet()) {
         Map<String,Long> values = allocation.resources.get(resource);
         Map<String,Long> values2 = resources.get(resource);
         if (values2 == null) {
            for (String subtype : values.keySet()) {
               this.setResourceUsage(resource, subtype, values.get(subtype));
            }
         } else {
            for (String subtype : values.keySet()) {
               long value1 = allocation.getResourceUsage(resource, subtype);
               Long value2 = getResourceUsage(resource, subtype);
               if (value2 == null) {
                  setResourceUsage(resource, subtype, value1);
               } else {
                  setResourceUsage(resource, subtype, Math.min(value1, value2));
               }
            }
         }
      }

      return minimum;
   }


   public Allocation add(Allocation allocation) {
      Allocation sum = clone();

      for (Resource resource : allocation.resources.keySet()) {
         Map<String,Long> values = allocation.resources.get(resource);
         Map<String,Long> values2 = resources.get(resource);
         if (values2 == null) {
            for (String subtype : values.keySet()) {
               this.setResourceUsage(resource, subtype, values.get(subtype));
            }
         } else {
            for (String subtype : values.keySet()) {
               long value1 = allocation.getResourceUsage(resource, subtype);
               Long value2 = getResourceUsage(resource, subtype);
               if (value2 == null) {
                  setResourceUsage(resource, subtype, value1);
               } else {
                  setResourceUsage(resource, subtype, value1 + value2);
               }
            }
         }
      }

      return sum;
   }
}