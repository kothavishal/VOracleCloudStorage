package com.vishal.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import oracle.cloud.storage.model.Key;

import com.vishal.agent.OracleStorageConnection;
import com.vishal.agent.OracleStorageConnectionMgr;
import com.vishal.api.OracleStorageServiceCredentials;
import com.vishal.api.Segment;
import com.vishal.api.ServiceType;
import com.vishal.api.Stream;
import com.vishal.api.SystemParams;
import com.vishal.api.Task;
import com.vishal.database.DataHolder;
import com.vishal.logging.Logger;
import com.vishal.util.Utils;

public class WorkflowRunner {

   private static final Logger LOGGER = Logger.getLogger(WorkflowRunner.class);

   public static final String MANIFEST_CONTAINER_NAME = "Manifest";

   public static WorkflowRunner workflowRunner = null;

   private WorkflowRunner() {

   }

   public static WorkflowRunner getWorkflowRunner() {
      if (workflowRunner == null) {
         synchronized (WorkflowRunner.class) {
            if (workflowRunner == null) {
               workflowRunner = new WorkflowRunner();
            }
         }
      }

      return workflowRunner;
   }

   public void createContainer(String key, OracleStorageServiceCredentials creds, Task task) {
      OracleStorageConnection connection = null;
      try {
         connection = OracleStorageConnectionMgr.getOracleStorageConnectionMgr().getConnection(creds.getServiceName());
         connection.connect(creds);
         createContainer(key, creds, task, connection);
      } catch (Exception e) {
         e.printStackTrace();
         throw new RuntimeException("Failed to create container " + key);
      } finally {
         OracleStorageConnectionMgr.getOracleStorageConnectionMgr().releaseConnection(connection);
      }
   }

   /**
    * Synchronized to prevent multiple users creating same Container 
    * @param key
    * @param creds
    * @param task
    * @param connection
    */
   public synchronized void createContainer(String key, OracleStorageServiceCredentials creds, Task task,
                  OracleStorageConnection connection) {
      if (!connection.containerExists(key)) {
         connection.createContainer(key, task);
      }
   }

   /**
    * Executes the PUT operation of the large object
    * @param key
    * @param input
    * @param creds
    * @param task
    * @return
    */
   public String executePut(String key, Stream input, OracleStorageServiceCredentials creds, Task task) {
      String msg = null;
      try {
         msg = performPut(key, input, creds, task);
      } catch (Exception e) {
         e.printStackTrace();
         msg = "Error occurred performing put operation of key " + key + ": " + e.getMessage();
      }
      return msg;
   }

   /**
    * Creates small segments and creates segment Threads and add them to the Segment Thread pool
    * Returns a SUCCESS message when all the segments are successfully uploaded
    * @param key
    * @param input
    * @param creds
    * @param task
    * @return
    */
   private String performPut(String key, Stream input, OracleStorageServiceCredentials creds, Task task) {

      LOGGER.debug("creating container " + key + " if it does not exist");
      createContainer(key, creds, task);

      // List<Segment> existingSegmentsFromManifest = getManifestSegments(key, creds, task);
      // Reading the Segments directly from the Container to support partial upload failure scenario
      List<Segment> existingSegmentsFromManifest = getAvailableSegments(key, creds, task);

      Map<String, Future<String>> futureTasks = new HashMap<String, Future<String>>();

      List<Segment> segments = Utils.getInputSegments(task.getId(), input, (SystemParams.SEGMENT_SIZE_IN_BYTES));
      List<String> segmentNames = new ArrayList<String>();

      for (int i = 0; i < segments.size(); i++) {

         Segment seg = segments.get(i);
         int index = Utils.findPos(existingSegmentsFromManifest, seg);
         Segment existingSeg = (index != -1) ? existingSegmentsFromManifest.get(index) : null;
         LOGGER.debug("requesting to upload " + seg);

         // if segment is already present in the Container
         if (seg.equals(existingSeg)) {
            existingSegmentsFromManifest.remove(existingSeg);
            task.appendTaskDetail("Segment '" + seg.getId() + "' already exists in the container '"
                           + existingSeg.getId() + "' with same md5sum. Skipping it's creation");
            segmentNames.add(existingSeg.getId());
            continue;
         }

         segmentNames.add(seg.getId());
         SegmentService segTask = new SegmentService(creds, seg, ServiceType.PUT, task);
         Future<String> submit = ClientExecutor.getClientExecutor().getSegmentThreadPoolExecutor().submit(segTask);
         task.appendTaskDetail("Submitting request to upload the segment '" + seg.getId() + "'");
         futureTasks.put(seg.getId(), submit);
      }

      // Tracks the status of each Segment upload
      for (Map.Entry<String, Future<String>> futureTask : futureTasks.entrySet()) {
         Future<String> future = futureTask.getValue();
         try {
            String segId = futureTask.getKey();
            String mess = future.get();
            if ("SUCCESS".equals(mess)) {
               String msg = "Successfully uploaded segment '" + segId + "'";
               task.appendTaskDetail(msg);
               LOGGER.debug(msg);
            } else {
               LOGGER.error(mess);
               task.appendTaskDetail(mess);
               return mess;
            }
         } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
         }
      }
      task.setSegmentNames(segmentNames);

      return "SUCCESS";
   }

   /**
    * Executes GET request of large object
    * @param key
    * @param creds
    * @param task
    * @return
    */
   public String executeGet(String key, OracleStorageServiceCredentials creds, Task task) {
      String msg = null;
      try {
         msg = performGet(key, creds, task);
      } catch (Exception e) {
         e.printStackTrace();
         msg = "Error occurred performing get operation on key " + key + ": " + e.getMessage();
      }
      return msg;
   }

   /**
    * It will look up Manifest Object and then creates Segment Threads accordingly which will download each segment.
    * Once all the segments are successfully downloaded, it will create a single stream of object and return back
    * @param key
    * @param creds
    * @param task
    * @return
    */
   public String performGet(String key, OracleStorageServiceCredentials creds, Task task) {

      Map<String, Future<String>> futureTasks = new HashMap<String, Future<String>>();

      List<Segment> segments = getManifestSegments(key, creds, task);
      if (segments.isEmpty()) {
         String msg = "Object '" + key + "' does not exist on the Oracle Cloud Storage Service";
         task.appendTaskDetail(msg);
         throw new RuntimeException(msg);
      }

      for (Segment seg : segments) {
         LOGGER.debug("requesting to download " + seg);
         SegmentService segTask = new SegmentService(creds, seg, ServiceType.GET, task);
         Future<String> submit = ClientExecutor.getClientExecutor().getSegmentThreadPoolExecutor().submit(segTask);
         futureTasks.put(seg.getId(), submit);
      }

      // Tracks the status of each Segment download
      for (Map.Entry<String, Future<String>> futureTask : futureTasks.entrySet()) {
         Future<String> future = futureTask.getValue();
         try {
            String segId = futureTask.getKey();
            String mess = future.get();
            if ("SUCCESS".equals(mess)) {
               String msg = "Successfully downloaded segment '" + segId + "'";
               LOGGER.debug(msg);
               task.appendTaskDetail(msg);
            } else {
               LOGGER.error(mess);
               task.appendTaskDetail(mess);
               return mess;
            }
         } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
         }
      }

      Stream stream = new Stream(key);
      String fileName = SystemParams.OUTPUT_DIR_LOCATION + "\\" + key;
      if (!fileName.contains(".txt")) {
         fileName = fileName + ".txt";
      }
      stream.setFileName(fileName);
      Utils.uploadSegmentsToFile(segments, key, fileName, task);
      task.appendTaskDetail("Downloaded File is saved to " + stream.getFileName());
      DataHolder.getDataHolder().getDownloadedObjects().put(key, stream);

      return "SUCCESS";
   }

   /**
    * Returns the segments based on the Container Manifest content
    * @param key
    * @param creds
    * @param task
    * @return
    */
   public List<Segment> getManifestSegments(String key, OracleStorageServiceCredentials creds, Task task) {

      String[] segmentNames = null;
      OracleStorageConnection conn = null;
      List<Segment> manifestSegments = new ArrayList<Segment>();
      try {
         conn = OracleStorageConnectionMgr.getOracleStorageConnectionMgr().getConnection(creds.getServiceName());
         conn.connect(creds);
         if (!conn.keyExistsInContainer(MANIFEST_CONTAINER_NAME, key)) {
            return manifestSegments;
         }
         Segment seg = new Segment(MANIFEST_CONTAINER_NAME);
         seg.setManifest(true);
         seg.setId(key);
         conn.downloadManifest(seg, task);
         String manifestSegmentNames = (String) seg.getOrigObj();
         segmentNames = manifestSegmentNames.split(",");

         for (String segKeyName : segmentNames) {
            if (conn.keyExistsInContainer(key, segKeyName)) {
               LOGGER.debug("Segment with Key name " + segKeyName + " already present in the container");
               Key segKey = conn.getKey(key, segKeyName);
               Segment existingSegment = new Segment(key);
               existingSegment.setId(segKeyName);
               existingSegment.setMd5Sum(segKey.getETag());
               manifestSegments.add(existingSegment);
            }
         }
      } catch (Exception e) {
         e.printStackTrace();
         throw new RuntimeException("Failed to get Manifest content of the object " + key + " " + e.getMessage());
      } finally {
         OracleStorageConnectionMgr.getOracleStorageConnectionMgr().releaseConnection(conn);
      }
      return manifestSegments;
   }

   /**
    * Returns the available segments in the Conatainer
    * @param key
    * @param creds
    * @param task
    * @return
    */
   public List<Segment> getAvailableSegments(String key, OracleStorageServiceCredentials creds, Task task) {

      OracleStorageConnection conn = null;
      List<Segment> manifestSegments = new ArrayList<Segment>();
      try {
         conn = OracleStorageConnectionMgr.getOracleStorageConnectionMgr().getConnection(creds.getServiceName());
         conn.connect(creds);
         List<Key> segKeysInContainer = conn.getKeysInContainer(key);

         for (Key segKey : segKeysInContainer) {
            if (segKey.getETag() == null) {
               continue;
            }
            LOGGER.debug("Segment '" + segKey.getKey() + "' already present in the container");
            Segment existingSegment = new Segment(key);
            existingSegment.setId(segKey.getKey());
            existingSegment.setMd5Sum(segKey.getETag());
            manifestSegments.add(existingSegment);
         }
      } catch (Exception e) {
         LOGGER.debug("Could not get available segments in the Container '" + key + "': " + e.getMessage());
         task.appendTaskDetail("Could not get available segments in the Container '" + key + "'");
      } finally {
         OracleStorageConnectionMgr.getOracleStorageConnectionMgr().releaseConnection(conn);
      }
      return manifestSegments;
   }

   /**
    * Deletes the Container and it's Segments
    * @param key
    * @param creds
    * @param task
    * @return
    */
   public String executeDeleteContainer(String key, OracleStorageServiceCredentials creds, Task task) {
      String msg = null;
      try {
         msg = performDeleteContainer(key, creds, task);
      } catch (Exception e) {
         e.printStackTrace();
         msg = "Error occurred performing delete operation of " + key + ": " + e.getMessage();
      }
      return msg;
   }

   public String performDeleteContainer(String key, OracleStorageServiceCredentials creds, Task task) {
      OracleStorageConnection connection = null;
      try {
         connection = OracleStorageConnectionMgr.getOracleStorageConnectionMgr().getConnection(creds.getServiceName());
         connection.connect(creds);
         connection.deleteContainer(key, task);
         connection.deleteManifestEntry(key);
      } catch (Exception e) {
         String msg = "Failed to delete the container '" + key + "': " + e.getMessage();
         task.appendTaskDetail(msg);
         e.printStackTrace();
         return msg;
      } finally {
         OracleStorageConnectionMgr.getOracleStorageConnectionMgr().releaseConnection(connection);
      }
      return "SUCCESS";
   }

   /**
    * Updates Manifest and deletes Unused Segments in the Container
    * @param manifestName
    * @param key
    * @param creds
    * @param segmentNames
    * @param task
    */
   public void updateManifestAndCleanUpContainer(String manifestName, String key,
                  OracleStorageServiceCredentials creds, List<String> segmentNames, Task task) {

      LOGGER.debug("updating manifest object");
      StringBuilder manifestObj = new StringBuilder();
      for (String segmentName : segmentNames) {
         manifestObj.append(segmentName + ",");
      }
      String finalManifestObj = manifestObj.toString();
      if (finalManifestObj.endsWith(",")) {
         finalManifestObj = finalManifestObj.substring(0, finalManifestObj.length() - 1);
      }
      OracleStorageConnection conn = null;
      try {
         conn = OracleStorageConnectionMgr.getOracleStorageConnectionMgr().getConnection(creds.getServiceName());
         conn.connect(creds);

         // Create Manifest Container if it does not exist
         createContainer(MANIFEST_CONTAINER_NAME, creds, task, conn);

         Segment seg = new Segment(manifestName);
         seg.setOrigObj(finalManifestObj);
         seg.setMd5Sum(Utils.getObjectMd5CheckSum(finalManifestObj));
         seg.setId(key);
         conn.uploadChunk(seg, task);

         task.appendTaskDetail("Updated Manifest for the Container '" + key + "' with '" + finalManifestObj + "'");

         // remove chunks which are not present in the manifest object
         conn.filterContainer(key, segmentNames, task);

      } catch (Exception e) {
         e.printStackTrace();
         throw new RuntimeException("manifest update of the object " + key + " failed with " + e.getMessage());
      } finally {
         OracleStorageConnectionMgr.getOracleStorageConnectionMgr().releaseConnection(conn);
      }
   }

   /**
    * Updates Manifest with Segment Names for the Contianer
    * @param manifestName
    * @param key
    * @param creds
    * @param segmentNames
    * @param task
    */
   public void updateManifest(String manifestName, String key, OracleStorageServiceCredentials creds,
                  List<String> segmentNames, Task task) {

      LOGGER.debug("updating manifest object");
      StringBuilder manifestObj = new StringBuilder();
      for (String segmentName : segmentNames) {
         manifestObj.append(segmentName + ",");
      }
      String finalManifestObj = manifestObj.toString();
      if (finalManifestObj.endsWith(",")) {
         finalManifestObj = finalManifestObj.substring(0, finalManifestObj.length() - 1);
      }
      OracleStorageConnection conn = null;
      try {
         conn = OracleStorageConnectionMgr.getOracleStorageConnectionMgr().getConnection(creds.getServiceName());
         conn.connect(creds);

         // Create Manifest Container if it does not exist
         createContainer(MANIFEST_CONTAINER_NAME, creds, task, conn);

         Segment seg = new Segment(manifestName);
         seg.setOrigObj(finalManifestObj);
         seg.setMd5Sum(Utils.getObjectMd5CheckSum(finalManifestObj));
         seg.setId(key);
         conn.uploadChunk(seg, task);
         task.appendTaskDetail("Updated Manifest for the Container '" + key + "' with '" + finalManifestObj + "'");
      } catch (Exception e) {
         e.printStackTrace();
         throw new RuntimeException("Manifest update of the object " + key + " failed with " + e.getMessage());
      } finally {
         OracleStorageConnectionMgr.getOracleStorageConnectionMgr().releaseConnection(conn);
      }
   }
}