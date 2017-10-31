package com.vishal.api;

import java.util.concurrent.Future;

import com.vishal.database.DataHolder;
import com.vishal.logging.Logger;
import com.vishal.service.ClientImpl;

public class TaskService {

   private static final Logger LOGGER = Logger.getLogger(TaskService.class);

   public static TaskService taskService = null;

   private TaskService() {

   }

   public static TaskService getTaskService() {
      if (taskService == null) {
         synchronized (TaskService.class) {
            if (taskService == null) {
               taskService = new TaskService();
            }
         }
      }
      return taskService;
   }

   public void cancelTask(Task task) {

      if (task.getStatus() == TaskStatus.COMPLETED || task.getStatus() == TaskStatus.ERROR) {
         throw new RuntimeException("cannot cancel a completed task");
      }
      Future<String> future = DataHolder.getDataHolder().getCurrentScheduledTasks().get(task.getId());
      LOGGER.error("cancelling task");
      future.cancel(true);
      return;
   }

   public void retryTask(Task task) {
      if (task.getStatus() == TaskStatus.COMPLETED) {
         throw new RuntimeException(task.getServiceType() + " Task for " + task.getKey()
                        + " is already completed and RETRY is not supported");
      }
      ClientImpl clientImpl = new ClientImpl(task.getCreds());
      if (task.getServiceType() == ServiceType.PUT) {
         clientImpl.put(task.getId(), task.getStream());
      } else if (task.getServiceType() == ServiceType.GET) {
         clientImpl.get(task.getKey());
      } else {
         clientImpl.deleteContainer(task.getKey());
      }
   }
}
