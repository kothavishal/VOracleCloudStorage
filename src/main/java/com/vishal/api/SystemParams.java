package com.vishal.api;

public class SystemParams {

   public static boolean DEBUG_ENABLED = false;
   public static int MAX_CONNECTIONS = 4;
   public static int SERVICE_THREAD_POOL_COUNT = 10;
   public static int SEGMENT_THREAD_POOL_COUNT = 10;
   public static int SEGMENT_SIZE_IN_BYTES = 25 * 1024; // 25KB
   public static boolean AUTO_RETRY = false;
   public static int MAX_NUM_OF_RETRIES = 0;
   public static int RETRY_WAIT_TIME_IN_SECS = 0;
   public static String OUTPUT_DIR_LOCATION = "C:\\ClientStorageOutput";
}
