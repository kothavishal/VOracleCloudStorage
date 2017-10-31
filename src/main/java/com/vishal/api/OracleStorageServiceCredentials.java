package com.vishal.api;

public class OracleStorageServiceCredentials {
   private String serviceName;
   private String userName;
   private String password;
   private String serviceUrl;

   public OracleStorageServiceCredentials(String userName, String password, String serviceName, String serviceUrl) {
      this.userName = userName;
      this.password = password;
      this.serviceName = serviceName;
      this.serviceUrl = serviceUrl;
   }

   public String getServiceName() {
      return serviceName;
   }

   public void setServiceName(String serviceName) {
      this.serviceName = serviceName;
   }

   public String getUserName() {
      return userName;
   }

   public void setUserName(String userName) {
      this.userName = userName;
   }

   public String getPassword() {
      return password;
   }

   public void setPassword(String password) {
      this.password = password;
   }

   public String getServiceUrl() {
      return serviceUrl;
   }

   public void setServiceUrl(String serviceUrl) {
      this.serviceUrl = serviceUrl;
   }

}
