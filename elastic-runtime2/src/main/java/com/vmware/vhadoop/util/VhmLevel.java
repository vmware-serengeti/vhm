package com.vmware.vhadoop.util;

import java.util.logging.Level;


public class VhmLevel extends java.util.logging.Level
{
   public static Level USER = new VhmLevel();

   protected VhmLevel() {
      super("USER", INFO.intValue() + 1);
   }
}
