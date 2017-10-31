package com.vishal.agent;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.vishal.api.SystemParams;
import com.vishal.logging.Logger;

public class OracleStorageConnectionMgr {

   private static final Logger LOGGER = Logger.getLogger(OracleStorageConnectionMgr.class);
   public static OracleStorageConnectionMgr oracleStorageConnMgr;
   private static Map<String, List<OracleStorageConnection>> connections = new ConcurrentHashMap<String, List<OracleStorageConnection>>();

   private OracleStorageConnectionMgr() {

   }

   public static OracleStorageConnectionMgr getOracleStorageConnectionMgr() {
      if (oracleStorageConnMgr == null) {
         synchronized (OracleStorageConnectionMgr.class) {
            if (oracleStorageConnMgr == null) {
               oracleStorageConnMgr = new OracleStorageConnectionMgr();
            }
         }
      }
      return oracleStorageConnMgr;
   }

   public OracleStorageConnection getConnection(String serviceName) {

      List<OracleStorageConnection> conns = null;
      OracleStorageConnection conn = null;
      synchronized (connections) {
         conns = connections.get(serviceName);

         LOGGER.debug("requested connection to oracle storage service " + serviceName);

         if (conns == null) {
            LOGGER.debug("creating new arraylist to store connections for " + serviceName);
            conns = new CopyOnWriteArrayList<OracleStorageConnection>();
            connections.put(serviceName, conns);
         }
      }

      synchronized (conns) {
         conn = getServiceConnection(conns, serviceName);
      }
      return conn;
   }

   public OracleStorageConnection getServiceConnection(List<OracleStorageConnection> conns, String serviceName) {
      OracleStorageConnection conn = null;
      int connsSize = conns.size();

      if (connsSize > 0) {
         LOGGER.debug("connections size for oracle storage service " + serviceName + " total " + connsSize);

         for (int i = 0; i < connsSize; i++) {
            OracleStorageConnection oc = conns.get(i);
            if (oc != null && !oc.isInUse()) {
               oc.setInUse(true);
               LOGGER.debug("reusing connection for oracle storage service " + serviceName + " - index " + i);
               return oc;
            } else if (oc == null) {
               LOGGER.debug("connection " + i + " is null");
            } else {
               LOGGER.debug("connection " + i + " is in use");
            }
         }
      } else {
         LOGGER.debug("no connections exist to service " + serviceName
                        + ". creating new connection for oracle storage service " + serviceName);
      }

      while (conns.size() >= SystemParams.MAX_CONNECTIONS) {
         LOGGER.debug("connections for oracle storage service " + serviceName + " exceeded maximum allowed: "
                        + SystemParams.MAX_CONNECTIONS);
         try {
            LOGGER.debug("waiting for someone to release connection");
            conns.wait();
         } catch (InterruptedException ie) {
            ie.printStackTrace();
         }
         LOGGER.debug("someone released connection and trying to get connection again");
         return getServiceConnection(conns, serviceName);
      }

      conn = new OracleStorageConnection();
      conn.setServiceName(serviceName);
      conn.setInUse(true);

      conns.add(conn);
      LOGGER.debug("new connection added at index: " + (conns.size() - 1));

      return conn;
   }

   public void releaseConnection(OracleStorageConnection conn) {
      LOGGER.debug("releasing device connection");
      if (conn == null) {
         LOGGER.debug("connection null. returning");
         return;
      }

      List<OracleStorageConnection> conns = connections.get(conn.getServiceName());

      if (conns == null || !conns.contains(conn)) {
         LOGGER.debug("connection does not exist");
         return;
      }

      try {
         LOGGER.debug("releasing connection " + conn.getServiceName() + " connections size " + conns.size());

         conn.setInUse(false);
         LOGGER.debug("closing and removing connection from pool " + conn.getServiceName());
         if (conn.isConnected()) {
            conn.disConnect();
         }
      } catch (Exception ex) {
         ex.printStackTrace();
         LOGGER.debug(ex.getMessage());
      } finally {
         if (conn.isConnected()) {
            conn.disConnect();
         }
      }

      synchronized (conns) {
         conns.remove(conn);
         conns.notifyAll();
      }

      conns = connections.get(conn.getServiceName());

      if (conns != null) {
         LOGGER.debug("released connection " + conn.getServiceName() + " connections size " + conns.size());
      }
   }

   public int getNumberOfFreeConnections(String serviceName) {
      List<OracleStorageConnection> conns = connections.get(serviceName);

      if (conns == null) {
         return 0;
      }

      int count = 0;
      for (OracleStorageConnection con : conns) {
         if (!con.isInUse()) {
            count++;
         }
      }
      return count;
   }
}
