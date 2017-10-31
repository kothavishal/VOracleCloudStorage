package com.vishal.service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.vishal.api.OracleStorageServiceCredentials;
import com.vishal.api.ServiceType;
import com.vishal.api.Stream;
import com.vishal.api.Task;
import com.vishal.api.TaskStatus;
import com.vishal.database.DataHolder;
import com.vishal.logging.Logger;

public class Workflow {

   private static final Logger LOGGER = Logger.getLogger(Workflow.class);

   public static Workflow workflow = null;

   private Workflow() {

   }

   public static Workflow getWorkflow() {

      if (workflow == null) {
         synchronized (Workflow.class) {
            if (workflow == null) {
               workflow = new Workflow();
            }
         }
      }
      return workflow;
   }

   public Task createService(String key, OracleStorageServiceCredentials creds, ServiceType serviceType) {
      return createService(key, null, creds, serviceType);
   }

   /**
    * Creates a Service Thread to perform PUT/GET/DELETE operations and adds these threads to the Service Thread Pool
    * Also creates a task corresponding to each request. Internally invokes WorkflowRunner to perform PUT/GET/DELETE
    * @param key
    * @param input
    * @param creds
    * @param serviceType
    * @return
    */
   public Task createService(String key, Stream input, OracleStorageServiceCredentials creds, ServiceType serviceType) {

      // Create a new Task for this Service Request
      Task task = new Task(key, serviceType, creds);
      task.setStream(input);

      addTaskToMap(task);

      WfEngineRunner wfEngineRunner = new WfEngineRunner(key, input, creds, serviceType, task);
      task.appendTaskDetail("Adding '" + key + "' " + serviceType.name()
                     + " operation to the service thread pool executor");
      Future<String> submit = ClientExecutor.getClientExecutor().getServiceThreadPoolExecutor().submit(wfEngineRunner);
      DataHolder.getDataHolder().addToFutureServiceTask(task, submit);
      DataHolder.getDataHolder().currentScheduledTasks.put(task.getId(), submit);

      task.appendTaskDetail("Task for " + serviceType.name() + " operation of the key '" + key
                     + "' successfully scheduled");
      return task;
   }

   /**
    * Tracks the Status of all the tasks submitted
    */
   public void trackTasksStatus() {
      Map<String, Task> successfullPutTask = new LinkedHashMap<>();
      for (Map.Entry<Task, Future<String>> futureTask : DataHolder.getDataHolder().getFutureServiceTasks().entrySet()) {
         Future<String> future = futureTask.getValue();
         Task task = futureTask.getKey();
         try {
            String mess = future.get();
            if ("SUCCESS".equals(mess)) {
               String msg = "Successfully finished " + task.getServiceType().name() + " task of '" + task.getKey()
                              + "'";
               task.appendTaskDetail(msg);
               LOGGER.debug(msg);
               if (task.getServiceType() == ServiceType.PUT) {
                  successfullPutTask.put(task.getKey(), task);
               }
            } else {
               LOGGER.error(mess);
               task.appendTaskDetail(mess);
            }
         } catch (InterruptedException | ExecutionException e) {
            String msg = "error occurred while reading task status " + e.getMessage();
            LOGGER.error(msg);
            e.printStackTrace();
            task.appendTaskDetail(msg);
            task.setStatus(TaskStatus.ERROR);
         } finally {
            DataHolder.getDataHolder().getCurrentScheduledTasks().remove(task.getId());
         }
      }

      /**
       * Update Manifest for all the successfully completed PUT Tasks
       * Makes sure the order of the requests is retained while updating
       */
      for (Task task : successfullPutTask.values()) {
         updateManifest(task);
      }

      // All the tasks are completed. Clear this data
      DataHolder.getDataHolder().getFutureServiceTasks().clear();
   }

   public void updateManifest(Task task) {
      try {
         WorkflowRunner.getWorkflowRunner().updateManifestAndCleanUpContainer(WorkflowRunner.MANIFEST_CONTAINER_NAME,
                        task.getKey(), task.getCreds(), task.getSegmentNames(), task);
      } catch (Exception e) {
         task.appendTaskDetail(e.getMessage());
      }
   }

   /**
    * Adds the task to the corresponding Map to keep track of each request
    * @param task
    */
   public void addTaskToMap(Task task) {
      if (task.getServiceType() == ServiceType.PUT) {
         addUploadTaskToMap(task);
      } else if (task.getServiceType() == ServiceType.GET) {
         addUploadTaskToMap(task);
      } else if (task.getServiceType() == ServiceType.DELETE) {
         addDeleteTaskToMap(task);
      }
   }

   public synchronized void addUploadTaskToMap(Task task) {
      List<Task> tasks = DataHolder.getDataHolder().getUploadTasks().get(task.getKey());
      if (tasks == null) {
         tasks = new ArrayList<Task>();
         DataHolder.getDataHolder().getUploadTasks().put(task.getKey(), tasks);
      }
      tasks.add(task);
   }

   public synchronized void addDownloadTaskToMap(Task task) {
      List<Task> tasks = DataHolder.getDataHolder().getUploadTasks().get(task.getKey());
      if (tasks == null) {
         tasks = new ArrayList<Task>();
         DataHolder.getDataHolder().getDownloadTasks().put(task.getKey(), tasks);
      }
      tasks.add(task);
   }

   public synchronized void addDeleteTaskToMap(Task task) {
      List<Task> tasks = DataHolder.getDataHolder().getUploadTasks().get(task.getKey());
      if (tasks == null) {
         tasks = new ArrayList<Task>();
         DataHolder.getDataHolder().getDeleteTasks().put(task.getKey(), tasks);
      }
      tasks.add(task);
   }
}

/**
 * Thread to perform PUT/GET/DELETE of large objects
 *
 */
class WfEngineRunner implements Callable<String> {

   private String key = null;
   private Stream input = null;
   private OracleStorageServiceCredentials creds = null;
   private ServiceType serviceType = null;
   private Task task = null;

   public WfEngineRunner(String key, Stream input, OracleStorageServiceCredentials creds, ServiceType serviceType,
                  Task task) {
      this.key = key;
      this.input = input;
      this.creds = creds;
      this.serviceType = serviceType;
      this.task = task;
   }

   public WfEngineRunner(String key, OracleStorageServiceCredentials creds, ServiceType serviceType) {
      this.key = key;
      this.creds = creds;
      this.serviceType = serviceType;
   }

   @Override
   public String call() {
      if (ServiceType.PUT == serviceType) {
         return WorkflowRunner.getWorkflowRunner().executePut(key, input, creds, task);
      } else if (ServiceType.GET == serviceType) {
         return WorkflowRunner.getWorkflowRunner().executeGet(key, creds, task);
      } else {
         return WorkflowRunner.getWorkflowRunner().executeDeleteContainer(key, creds, task);
      }
   }
}
