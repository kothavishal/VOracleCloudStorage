package com.vishal.service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.vishal.api.SystemParams;

public class ClientExecutor {

   public static ClientExecutor clientExecutor = null;
   private ExecutorService serviceThreadPoolExecutor = null;
   private ExecutorService segmentThreadPoolExecutor = null;

   private ClientExecutor() {

   }

   public static ClientExecutor getClientExecutor() {
      if (clientExecutor == null) {
         synchronized (ClientExecutor.class) {
            if (clientExecutor == null) {
               clientExecutor = new ClientExecutor();
            }
         }
      }
      return clientExecutor;
   }

   public void initThreadPools() {
      serviceThreadPoolExecutor = Executors.newFixedThreadPool(SystemParams.SERVICE_THREAD_POOL_COUNT);
      segmentThreadPoolExecutor = Executors.newFixedThreadPool(SystemParams.SEGMENT_THREAD_POOL_COUNT);

   }

   public ExecutorService getServiceThreadPoolExecutor() {
      return serviceThreadPoolExecutor;
   }

   public void setServiceThreadPoolExecutor(ExecutorService serviceThreadPoolExecutor) {
      this.serviceThreadPoolExecutor = serviceThreadPoolExecutor;
   }

   public ExecutorService getSegmentThreadPoolExecutor() {
      return segmentThreadPoolExecutor;
   }

   public void setSegmentThreadPoolExecutor(ExecutorService segmentThreadPoolExecutor) {
      this.segmentThreadPoolExecutor = segmentThreadPoolExecutor;
   }
}
