package com.vishal.api;

import java.util.List;
import java.util.UUID;

public class Task {

   private String id;
   private ServiceType serviceType;
   private String key;
   private TaskStatus status;
   private int percentComplete;
   private StringBuilder taskDetail;
   private long startTime;
   private long endTime;
   private OracleStorageServiceCredentials creds;
   private List<String> segmentNames;
   // stream will be saved only for PUT tasks
   private Stream stream = null;

   public Task(String key, ServiceType serviceType, OracleStorageServiceCredentials creds) {
      this.startTime = System.currentTimeMillis();
      this.key = key;
      this.serviceType = serviceType;
      status = TaskStatus.NOT_STARTED;
      this.id = UUID.randomUUID().toString();
      taskDetail = new StringBuilder();
      this.creds = creds;
   }

   public String getId() {
      return id;
   }

   public void setId(String id) {
      this.id = id;
   }

   public void appendTaskDetail(String msg) {
      System.out.println(id + ": " + msg);
      taskDetail.append(msg);
      taskDetail.append("\n");
   }

   public StringBuilder getTaskDetail() {
      return taskDetail;
   }

   public void setTaskDetail(StringBuilder taskDetail) {
      this.taskDetail = taskDetail;
   }

   public int getPercentComplete() {
      return percentComplete;
   }

   public void setPercentComplete(int percentComplete) {
      this.percentComplete = percentComplete;
   }

   public TaskStatus getStatus() {
      return status;
   }

   public void setStatus(TaskStatus status) {
      this.status = status;
   }

   public String getKey() {
      return key;
   }

   public void setKey(String key) {
      this.key = key;
   }

   public ServiceType getServiceType() {
      return serviceType;
   }

   public void setServiceType(ServiceType serviceType) {
      this.serviceType = serviceType;
   }

   public long getStartTime() {
      return startTime;
   }

   public void setStartTime(long startTime) {
      this.startTime = startTime;
   }

   public long getEndTime() {
      return endTime;
   }

   public void setEndTime(long endTime) {
      this.endTime = endTime;
   }

   public OracleStorageServiceCredentials getCreds() {
      return creds;
   }

   public void setCreds(OracleStorageServiceCredentials creds) {
      this.creds = creds;
   }

   public List<String> getSegmentNames() {
      return segmentNames;
   }

   public void setSegmentNames(List<String> segmentNames) {
      this.segmentNames = segmentNames;
   }

   public Stream getStream() {
      return stream;
   }

   public void setStream(Stream stream) {
      this.stream = stream;
   }

   @Override
   public String toString() {
      return getId();
   }
}
