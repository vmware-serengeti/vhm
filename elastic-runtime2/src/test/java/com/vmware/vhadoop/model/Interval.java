package com.vmware.vhadoop.model;

/**
 * This describes an interval to a workload. When used to describe a period, that period is considered
 * lapsed when either the specified duration or the specified number of instructions has been reached.
 */
class Interval {
   long instructions = Long.MAX_VALUE;
   long millis = Long.MAX_VALUE;

   public Interval() {
      this.instructions = Long.MAX_VALUE;
      this.millis = Long.MAX_VALUE;
   }

   private Interval(Interval interval) {
      this.millis = interval.millis;
      this.instructions = interval.instructions;
   }

   @Override
   public Interval clone() {
      return new Interval(this);
   }

   public Interval setDuration(long millis) {
      this.millis = millis;
      return this;
   }

   public Interval setInstructions(long instructions) {
      this.instructions = instructions;
      return this;
   }

   public long getDuration() {
      return millis;
   }

   public long getInstructions() {
      return instructions;
   }

   public Interval minimum(Interval interval) {
      Interval minimum = clone();
      if (minimum.millis > interval.millis) {
         minimum.millis = interval.millis;
      }

      if (minimum.instructions > interval.instructions) {
         minimum.instructions = interval.instructions;
      }

      return minimum;
   }
}