package com.vishal.service;

import java.util.concurrent.Callable;

import com.vishal.agent.OracleStorageConnection;
import com.vishal.agent.OracleStorageConnectionMgr;
import com.vishal.api.OracleStorageServiceCredentials;
import com.vishal.api.Segment;
import com.vishal.api.ServiceType;
import com.vishal.api.SystemParams;
import com.vishal.api.Task;
import com.vishal.logging.Logger;

public class SegmentService implements Callable<String> {

   private static final Logger LOGGER = Logger.getLogger(SegmentService.class);
   private Segment segment = null;
   private OracleStorageServiceCredentials creds;
   private ServiceType serviceType = null;
   private Task task;

   public SegmentService(OracleStorageServiceCredentials creds, Segment segment, ServiceType serviceType, Task task) {
      this.creds = creds;
      this.segment = segment;
      this.serviceType = serviceType;
      this.task = task;
   }

   public String call() throws Exception {
      OracleStorageConnection conn = null;
      boolean releaseConnection = false;
      int retryCount = 0;
      do {
         try {
            LOGGER.debug("getting connection for " + segment.getId());
            conn = getConnection();
            if (serviceType == ServiceType.PUT) {
               conn.uploadChunk(segment, task);
            } else if (serviceType == ServiceType.GET) {
               conn.downloadChunk(segment, task);
            }
            releaseConnection = true;
            break;
         } catch (Exception e) {
            String msg = "error " + e.getMessage() + "occurred while performing " + serviceType.name()
                           + " task for the segment " + segment.getId() + " " + retryCount + "time";
            // If no Auto RETRY or if Number of RETRIES exceeded, return the failure message
            if (!SystemParams.AUTO_RETRY || retryCount > SystemParams.MAX_NUM_OF_RETRIES) {
               LOGGER.error(msg);
               releaseConnection = true;
               return msg;
            } else {
               task.appendTaskDetail(msg);
            }
         } finally {
            LOGGER.debug("releasing connection " + conn + " for the key" + segment.getId());
            if (releaseConnection) {
               OracleStorageConnectionMgr.getOracleStorageConnectionMgr().releaseConnection(conn);
            }
         }
         sleep(SystemParams.RETRY_WAIT_TIME_IN_SECS);
         retryCount++;
      } while (SystemParams.AUTO_RETRY);
      return "SUCCESS";
   }

   private void sleep(int secs) {
      try {
         Thread.sleep(secs * 1000);
      } catch (InterruptedException iE) {
         LOGGER.error("Thread interrupted while sleeping" + iE);
      }
   }

   public OracleStorageConnection getConnection() {
      OracleStorageConnection conn = OracleStorageConnectionMgr.getOracleStorageConnectionMgr().getConnection(
                     creds.getServiceName());
      LOGGER.debug("got connection " + conn + " for key " + segment.getId());
      conn.connect(creds);

      return conn;
   }
}
