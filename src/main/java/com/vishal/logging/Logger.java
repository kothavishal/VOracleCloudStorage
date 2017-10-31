package com.vishal.logging;

import java.util.Date;

import com.vishal.api.SystemParams;

public class Logger {

   public static Logger logger = null;
   private Class<?> clazz = null;

   private Logger() {
   }

   public static Logger getLogger(Class<?> clazz) {
      if (logger == null) {
         synchronized (Logger.class) {
            if (logger == null) {
               logger = new Logger();
            }
         }
      }
      logger.clazz = clazz;
      return logger;
   }

   public void debug(String msg) {
      if (!SystemParams.DEBUG_ENABLED) {
         return;
      }
      Date d = new Date(System.currentTimeMillis());
      System.out.println(d + ": " + clazz.getName() + ": " + msg);
   }

   public void info(String msg) {
      Date d = new Date(System.currentTimeMillis());
      System.out.println(d + ": " + clazz.getName() + ": " + msg);
   }

   public void error(String msg) {
      Date d = new Date(System.currentTimeMillis());
      System.out.println(d + ": " + clazz.getName() + ": " + msg);
   }
}
