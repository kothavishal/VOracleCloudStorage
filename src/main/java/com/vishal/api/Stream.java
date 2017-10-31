package com.vishal.api;

import java.io.Serializable;

public class Stream implements Serializable {

   private static final long serialVersionUID = 1L;
   private String id;
   // Contains absolute path of the File along wit Filename
   private String fileName;
   private String path;
   private long size;

   public Stream(String id) {
      this.id = id;
   }

   public long getSize() {
      return size;
   }

   public void setSize(long size) {
      this.size = size;
   }

   public String getId() {
      return id;
   }

   public void setId(String id) {
      this.id = id;
   }

   public String getFileName() {
      return fileName;
   }

   public void setFileName(String fileName) {
      this.fileName = fileName;
   }

   public String getPath() {
      return path;
   }

   public void setPath(String path) {
      this.path = path;
   }
}
