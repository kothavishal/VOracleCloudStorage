package com.vishal.agent;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import oracle.cloud.storage.CloudStorage;
import oracle.cloud.storage.CloudStorageConfig;
import oracle.cloud.storage.CloudStorageFactory;
import oracle.cloud.storage.model.Container;
import oracle.cloud.storage.model.Key;
import oracle.cloud.storage.model.QueryOption;
import oracle.cloud.storage.model.StorageInputStream;

import com.vishal.api.OracleStorageServiceCredentials;
import com.vishal.api.Segment;
import com.vishal.api.SystemParams;
import com.vishal.api.Task;
import com.vishal.logging.Logger;
import com.vishal.util.Utils;

public class OracleStorageConnection {

   private static final Logger LOGGER = Logger.getLogger(OracleStorageConnection.class);
   CloudStorage myConnection = null;
   private String serviceName = null;
   private boolean isInUse = false;
   private boolean isConnected = false;

   public String getServiceName() {
      return serviceName;
   }

   public void setServiceName(String serviceName) {
      this.serviceName = serviceName;
   }

   public boolean isInUse() {
      return isInUse;
   }

   public void setInUse(boolean isInUse) {
      this.isInUse = isInUse;
   }

   public boolean isConnected() {
      return isConnected;
   }

   public void disConnect() {
      myConnection = null;
      isConnected = false;
   }

   /**
    * Establishes Connection to Oracle Cloud Storage Service
    * @param creds
    * @return
    */
   public boolean connect(OracleStorageServiceCredentials creds) {
      if (isConnected()) {
         return true;
      }
      try {
         LOGGER.debug("connecting to the oracle storage service " + creds.getServiceName() + " with url "
                        + creds.getServiceUrl() + " using the username " + creds.getUserName() + " and password "
                        + creds.getPassword());
         CloudStorageConfig myConfig = new CloudStorageConfig();
         myConfig.setServiceName(creds.getServiceName());
         myConfig.setUsername(creds.getUserName());
         myConfig.setPassword(creds.getPassword().toCharArray());
         myConfig.setServiceUrl(creds.getServiceUrl());

         myConnection = CloudStorageFactory.getStorage(myConfig);
      } catch (MalformedURLException e) {
         LOGGER.error("failed to get connection to the service " + creds.getServiceName());
         e.printStackTrace();
         return false;
      }
      isConnected = true;
      return true;
   }

   /**
    * Uploads the Segment on to Oracle Cloud Service
    * @param segment
    * @param task
    */
   public void uploadChunk(Segment segment, Task task) {

      task.appendTaskDetail("Uploading segment '" + segment.getId() + " "
                     + (segment.getSize() > 0 ? "(" + segment.getSize() + " Bytes)" : "") + "' to the Container '"
                     + segment.getCusId() + "' to Oracle Cloud Service");

      InputStream is = Utils.convertObject(segment.getOrigObj());
      Key key = myConnection.storeObject(segment.getCusId(), segment.getId(), "text/plain", is);

      if (key.getETag().equals(segment.getMd5Sum())) {
         LOGGER.debug("Successfully uploaded segment '" + key.getKey() + "' to Oracle Cloud Service container "
                        + segment.getCusId() + " etag is " + key.getETag());
      } else {
         task.appendTaskDetail("Uploaded segment md5: '" + key.getETag() + "' does not match with expected md5: "
                        + segment.getMd5Sum());
         throw new RuntimeException("upload of segment file seems to be corrupted as md5sum did not match");
      }

   }

   /**
    * Downloads the Segment from Oracle Cloud Service and saves to a File
    * @param segment
    * @param task
    */
   public void downloadChunk(Segment segment, Task task) {

      task.appendTaskDetail("Downloading segment '" + segment.getId() + "' from the Container '" + segment.getCusId()
                     + "' from Oracle Cloud Service");

      StorageInputStream objStream = myConnection.retrieveObject(segment.getCusId(), segment.getId());
      LOGGER.debug("downloaded segment " + segment.getId() + " to Oracle Cloud Service " + objStream);
      Key key = myConnection.describeObject(segment.getCusId(), segment.getId());
      try {
         ObjectInput ois = new ObjectInputStream(objStream);
         Object obj = ois.readObject();

         segment.setFileName(SystemParams.OUTPUT_DIR_LOCATION + "\\" + segment.getId());
         Utils.saveSegmentToFile(segment.getFileName(), obj);
         String downloadMd5 = Utils.getObjectMd5CheckSum(obj);
         if (key.getETag().equals(downloadMd5)) {
            LOGGER.debug("Successfully downloaded segment " + key.getKey() + " from Oracle Cloud Service container "
                           + segment.getCusId() + " and md5sum is " + key.getETag());
         } else {
            task.appendTaskDetail("Downloaded segment md5: '" + downloadMd5 + "' does not match with expected md5: "
                           + key.getETag());
            throw new RuntimeException("Download of segment file seems to be corrupted as md5sum did not match");
         }
      } catch (IOException | ClassNotFoundException e) {
         e.printStackTrace();
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   /**
    * Downloads Manifest Object from Oracle Cloud Service and sets the Object to segment Object
    * @param segment
    * @param task
    */
   public void downloadManifest(Segment segment, Task task) {

      task.appendTaskDetail("Downloading segment '" + segment.getId() + "' from the Container '" + segment.getCusId()
                     + "' from Oracle Cloud Service");

      StorageInputStream objStream = myConnection.retrieveObject(segment.getCusId(), segment.getId());
      LOGGER.debug("downloaded segment " + segment.getId() + " to Oracle Cloud Service " + objStream);
      myConnection.describeObject(segment.getCusId(), segment.getId());
      try {
         ObjectInput ois = new ObjectInputStream(objStream);
         Object obj = ois.readObject();
         segment.setOrigObj(obj);
      } catch (IOException | ClassNotFoundException e) {
         e.printStackTrace();
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   /**
    * Creates a Container on Oracle Cloud Storage Service 
    * @param name
    * @param task
    */
   public void createContainer(String name, Task task) {
      task.appendTaskDetail("Creating new container '" + name + "'");
      myConnection.createContainer(name, new HashMap<String, String>());
   }

   /**
    * Checks if the Container exists in Oracle Cloud Storage Service
    * @param name
    * @return
    */
   public boolean containerExists(String name) {
      List<Container> containers = myConnection.listContainers();
      for (Container container : containers) {
         if (name.equals(container.getName())) {
            LOGGER.debug("container " + name + "  exists");
            return true;
         }
      }
      return false;
   }

   /**
    * Deletes complete Container from the Oracle Cloud Storage Service
    * @param name
    * @param task
    */
   public void deleteContainer(String name, Task task) {
      Container container = myConnection.describeContainer(name);
      Map<QueryOption, String> queryOptions = new HashMap<QueryOption, String>();
      List<Key> keys = myConnection.listObjects(container.getName(), queryOptions);
      for (Key key : keys) {
         task.appendTaskDetail("Deleting Segment '" + key.getKey() + "'");
         myConnection.deleteObject(name, key.getKey());
      }
      task.appendTaskDetail("Deleting container '" + name + "'");
      myConnection.deleteContainer(name);
   }

   /**
    * Deletes all the Segments from the Container except Segments provided in the List segmentsToRemain
    * @param name
    * @param segmentsToRemain
    * @param task
    */
   public void filterContainer(String name, List<String> segmentsToRemain, Task task) {
      Container container = myConnection.describeContainer(name);
      Map<QueryOption, String> queryOptions = new HashMap<QueryOption, String>();
      List<Key> keys = myConnection.listObjects(container.getName(), queryOptions);
      for (Key key : keys) {
         if (!segmentsToRemain.contains(key.getKey())) {
            task.appendTaskDetail("Deleting Segment '" + key.getKey() + "'");
            myConnection.deleteObject(name, key.getKey());
         }
      }
   }

   /**
    * Deletes Entry from the Manifest Container
    * @param name
    */
   public void deleteManifestEntry(String name) {
      myConnection.deleteObject("Manifest", name);
   }

   /**
    * Deletes Segment from the Container
    * @param name
    * @param segmentKey
    */
   public void deleteSegment(String name, String segmentKey) {
      myConnection.deleteObject(name, segmentKey);
   }

   /**
    * Checks whether given Key is present in the Container
    * @param containerName
    * @param keyName
    * @return
    */
   public boolean keyExistsInContainer(String containerName, String keyName) {
      Container container = myConnection.describeContainer(containerName);
      Map<QueryOption, String> queryOptions = new HashMap<QueryOption, String>();
      List<Key> keys = myConnection.listObjects(container.getName(), queryOptions);
      for (Key key : keys) {
         if (keyName.equals(key.getKey())) {
            return true;
         }
      }
      return false;
   }

   /**
    * Returns Key Object of the given Key Name
    * @param containerName
    * @param keyName
    * @return
    */
   public Key getKey(String containerName, String keyName) {
      return myConnection.describeObject(containerName, keyName);
   }

   /**
    * Returns the List of Keys present in the given Container
    * @param containerName
    * @return
    */
   public List<Key> getKeysInContainer(String containerName) {
      Map<QueryOption, String> queryOptions = new HashMap<QueryOption, String>();
      List<Key> keysInContainer = new ArrayList<Key>();
      List<Key> sumKeysInContainer = myConnection.listObjects(containerName, queryOptions);
      for (Key sumKey : sumKeysInContainer) {
         keysInContainer.add(myConnection.describeObject(containerName, sumKey.getKey()));
      }
      return keysInContainer;
   }

   /**
    * prints Container and it's Segment Keys
    */
   public void printContainers() {
      List<Container> containers = myConnection.listContainers();
      for (Container container : containers) {
         LOGGER.debug("conainer found during createContainer " + container.getName() + " and count "
                        + container.getCount() + " and size " + container.getSize());
         Map<QueryOption, String> queryOptions = new HashMap<QueryOption, String>();
         List<Key> keys = myConnection.listObjects(container.getName(), queryOptions);
         for (Key key : keys) {
            LOGGER.debug("key found " + key.getKey() + " and its value is ");
         }
      }
   }
}