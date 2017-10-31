package com.vishal.service;

import com.vishal.api.OracleStorageServiceCredentials;
import com.vishal.api.ServiceType;
import com.vishal.api.Stream;
import com.vishal.client.IClient;
import com.vishal.database.DataHolder;

public class ClientImpl implements IClient {

   OracleStorageServiceCredentials creds;

   public ClientImpl(OracleStorageServiceCredentials creds) {
      this.creds = creds;
   }

   /**
    * API to perform PUT of large objects
    */
   public void put(String key, Stream input) {
      Workflow.getWorkflow().createService(key, input, creds, ServiceType.PUT);
   }

   /**
    * API to perform GET of large objects
    */
   public Stream get(String key) {
      Workflow.getWorkflow().createService(key, creds, ServiceType.GET);
      return DataHolder.getDataHolder().getDownloadedObjects().get(key);
   }

   /**
    * API to perform DELETE of large objects
    */
   public void deleteContainer(String key) {
      Workflow.getWorkflow().createService(key, creds, ServiceType.DELETE);
      return;
   }
}
