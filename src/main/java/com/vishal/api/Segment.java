package com.vishal.api;

public class Segment {

   private String cusId;
   private String id;
   private Object origObj;
   private String md5Sum;
   private int size;
   // Contains absolute path of the File along wit Filename
   private String fileName;
   private boolean isManifest;

   public Segment(String cusId) {
      this.cusId = cusId;
   }

   public String getCusId() {
      return cusId;
   }

   public void setCusId(String cusId) {
      this.cusId = cusId;
   }

   public String getId() {
      return id;
   }

   public void setId(String id) {
      this.id = id;
   }

   public Object getOrigObj() {
      return origObj;
   }

   public void setOrigObj(Object origObj) {
      this.origObj = origObj;
   }

   public String getMd5Sum() {
      return md5Sum;
   }

   public void setMd5Sum(String md5Sum) {
      this.md5Sum = md5Sum;
   }

   public int getSize() {
      return size;
   }

   public void setSize(int size) {
      this.size = size;
   }

   public String getFileName() {
      return fileName;
   }

   public void setFileName(String fileName) {
      this.fileName = fileName;
   }

   public boolean isManifest() {
      return isManifest;
   }

   public void setManifest(boolean isManifest) {
      this.isManifest = isManifest;
   }

   @Override
   public String toString() {
      return id;
   }

   @Override
   public boolean equals(Object o) {
      if (o instanceof Segment) {
         Segment seg = (Segment) o;
         return seg.getCusId().equals(this.getCusId()) && seg.getMd5Sum().equals(this.getMd5Sum());
      }
      return false;
   }
}
