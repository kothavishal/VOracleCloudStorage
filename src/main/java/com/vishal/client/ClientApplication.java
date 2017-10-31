package com.vishal.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import com.vishal.api.OracleStorageServiceCredentials;
import com.vishal.api.Stream;
import com.vishal.api.SystemParams;
import com.vishal.service.ClientExecutor;
import com.vishal.service.ClientImpl;
import com.vishal.service.Workflow;

public class ClientApplication {

   public static void main(String[] args) {

      System.out.println("******************************************************************************");
      System.out.println("****Running Client Application to communicate with Oracle Storage Service ****");
      System.out.println("******************************************************************************");

      Scanner scan = new Scanner(System.in);

      takeSystemInputs(scan);

      // Initialize Thread pools
      ClientExecutor.getClientExecutor().initThreadPools();

      IClient client = takeClientInputs(scan);

      List<Stream> uploads = new ArrayList<Stream>();
      List<String> downloads = new ArrayList<String>();
      List<String> deletes = new ArrayList<String>();

      boolean first = true;
      while (true) {
         takeObjectInputs(scan, uploads, downloads, deletes, first);

         for (Stream input : uploads) {
            client.put(input.getId(), input);
         }
         uploads.clear();

         for (String input : downloads) {
            client.get(input);
         }
         downloads.clear();

         for (String input : deletes) {
            client.deleteContainer(input);
         }
         deletes.clear();

         // Tracks all the tasks scheduled above
         Workflow.getWorkflow().trackTasksStatus();
         first = false;
         System.out.println("Do you wish to perform more operations (y/n)? ");
         if ("n".equalsIgnoreCase(scan.next())) {
            break;
         }
      }
      System.out.println("All the tasks have been completed and shutting down the application");
      ClientExecutor.getClientExecutor().getServiceThreadPoolExecutor().shutdown();
      ClientExecutor.getClientExecutor().getSegmentThreadPoolExecutor().shutdown();

      scan.close();
   }

   public static void takeSystemInputs(Scanner scan) {

      System.out.println("Do you wish to control the System inputs (y/n)? If NO, System would use Default values:");
      if ("n".equalsIgnoreCase(scan.next())) {
         return;
      }

      System.out.println("Do you wish to run the application in DEBUG mode (y/n)?");
      if ("y".equalsIgnoreCase(scan.next())) {
         SystemParams.DEBUG_ENABLED = true;
      }

      System.out.println("Do you wish to control the Thread Pools and Connection Pool Sizes (y/n)?");
      if ("y".equalsIgnoreCase(scan.next())) {
         System.out.println("Services Thread Pool size: ");
         SystemParams.SERVICE_THREAD_POOL_COUNT = scan.nextInt();

         System.out.println("Segments Thread Pool size: ");
         SystemParams.SEGMENT_THREAD_POOL_COUNT = scan.nextInt();

         System.out.println("Max Connection Pool size: ");
         SystemParams.MAX_CONNECTIONS = scan.nextInt();
      }
      System.out.println("Do you wish to perform Auto RETRY on failures (y/n)?");
      String autoRetry = scan.next();
      if ("y".equals(autoRetry)) {
         SystemParams.AUTO_RETRY = true;

         System.out.println("Number of times to RETRY:");
         SystemParams.MAX_NUM_OF_RETRIES = scan.nextInt();

         System.out.println("Wait time for each RETRY in secs: ");
         SystemParams.RETRY_WAIT_TIME_IN_SECS = scan.nextInt();
      }
      System.out.println("Do you wish to control Segment's Size (y/n)?");
      String controlSegSize = scan.next();
      if ("y".equals(controlSegSize)) {
         System.out.println("Please enter the Segment size in (KB or MB or GB) For Eg: 100KB: ");
         String segmentSize = scan.next();
         int size = 25 * 1024;
         if (segmentSize.contains("KB")) {
            segmentSize = segmentSize.replace("KB", "").trim();
            size = Integer.valueOf(segmentSize) * 1024;
         } else if (segmentSize.contains("MB")) {
            segmentSize = segmentSize.replace("MB", "").trim();
            size = Integer.valueOf(segmentSize) * 1024 * 1024;
         } else if (segmentSize.contains("GB")) {
            size = Integer.valueOf(segmentSize) * 1024 * 1024 * 1024;
         }
         SystemParams.SEGMENT_SIZE_IN_BYTES = size;
      }
      System.out.println("Do you wish to enter Directory location to Save Downloaded Objects (y/n)?");
      String controlOutputFileName = scan.next();
      if ("y".equals(controlOutputFileName)) {
         System.out.println("Please enter the Directory absolute path to Store downloaded Objects: ");
         SystemParams.OUTPUT_DIR_LOCATION = scan.next();
      }
   }

   public static IClient takeClientInputs(Scanner scan) {

      OracleStorageServiceCredentials credsUser = new OracleStorageServiceCredentials("kothavishal@gmail.com",
                     "Viks$3549", "kothavishal-kothavishal", "https://kothavishal.storage.oraclecloud.com/");
      System.out.println("Do you wish to enter Oracle Storage Credentials (y/n)? If NO, System would use Default Credentials:");
      if ("y".equalsIgnoreCase(scan.next())) {
         System.out.println("Service Name: ");
         credsUser.setServiceName(scan.next());

         System.out.println("User Name: ");
         credsUser.setUserName(scan.next());

         System.out.println("Password: ");
         credsUser.setPassword(scan.next());

         System.out.println("Service URL: ");
         credsUser.setServiceUrl(scan.next());
      }
      IClient client = new ClientImpl(credsUser);
      return client;
   }

   public static void takeObjectInputs(Scanner scan, List<Stream> uploads, List<String> downloads,
                  List<String> deletes, boolean first) {

      System.out.println("How many PUT operations you wish to perform (1-10)? ");
      int putOperations = scan.nextInt();
      while (putOperations > 0) {
         System.out.println("   Please enter Key Name:");
         Stream inputStream = new Stream(scan.next());
         System.out.println("   Please enter Object File absolute path location:");
         inputStream.setFileName(scan.next());
         uploads.add(inputStream);
         putOperations -= 1;
      }
      if (first) {
         return;
      }
      System.out.println("How many GET operations you wish to perform (1-10)? ");
      int getOperations = scan.nextInt();
      while (getOperations > 0) {
         System.out.println("   Please enter Key Name:");
         downloads.add(scan.next());
         getOperations -= 1;
      }

      System.out.println("How many DELETE operations you wish to perform (1-10)? ");
      int deleteOperations = scan.nextInt();
      while (deleteOperations > 0) {
         System.out.println("   Please enter Key Name:");
         deletes.add(scan.next());
         deleteOperations -= 1;
      }
   }
}
