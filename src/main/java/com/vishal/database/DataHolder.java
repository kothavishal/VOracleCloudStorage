package com.vishal.database;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import com.vishal.api.Stream;
import com.vishal.api.Task;

public class DataHolder {

   public static DataHolder dataHolder = null;

   private DataHolder() {

   }

   public static DataHolder getDataHolder() {

      if (dataHolder == null) {
         synchronized (DataHolder.class) {
            if (dataHolder == null) {
               dataHolder = new DataHolder();
            }
         }
      }
      return dataHolder;
   }

   private Map<String, List<Task>> uploadTasks = new HashMap<String, List<Task>>();
   private Map<String, List<Task>> downloadTasks = new HashMap<String, List<Task>>();
   private Map<String, List<Task>> deleteTasks = new HashMap<String, List<Task>>();
   private Map<String, String> md5Sums = new HashMap<String, String>();
   private Map<String, Stream> downloadedObjects = new HashMap<>();
   public Map<String, Future<String>> currentScheduledTasks = new HashMap<String, Future<String>>();
   private Map<Task, Future<String>> futureServiceTasks = new LinkedHashMap<Task, Future<String>>();

   public Map<String, Future<String>> getCurrentScheduledTasks() {
      return currentScheduledTasks;
   }

   public void setCurrentScheduledTasks(Map<String, Future<String>> currentScheduledTasks) {
      this.currentScheduledTasks = currentScheduledTasks;
   }

   public Map<String, List<Task>> getUploadTasks() {
      return uploadTasks;
   }

   public void setUploadTasks(Map<String, List<Task>> uploadTasks) {
      this.uploadTasks = uploadTasks;
   }

   public Map<String, List<Task>> getDownloadTasks() {
      return downloadTasks;
   }

   public void setDownloadTasks(Map<String, List<Task>> downloadTasks) {
      this.downloadTasks = downloadTasks;
   }

   public Map<String, List<Task>> getDeleteTasks() {
      return deleteTasks;
   }

   public void setDeleteTasks(Map<String, List<Task>> deleteTasks) {
      this.deleteTasks = deleteTasks;
   }

   public Map<String, String> getMd5Sums() {
      return md5Sums;
   }

   public void setMd5Sums(Map<String, String> md5Sums) {
      this.md5Sums = md5Sums;
   }

   public Map<String, Stream> getDownloadedObjects() {
      return downloadedObjects;
   }

   public void setDownloadedObjects(Map<String, Stream> downloadedObjects) {
      this.downloadedObjects = downloadedObjects;
   }

   public void addToFutureServiceTask(Task task, Future<String> submit) {
      futureServiceTasks.put(task, submit);
   }

   public Map<Task, Future<String>> getFutureServiceTasks() {
      return futureServiceTasks;
   }

   public void setFutureServiceTasks(Map<Task, Future<String>> futureServiceTasks) {
      this.futureServiceTasks = futureServiceTasks;
   }
}
