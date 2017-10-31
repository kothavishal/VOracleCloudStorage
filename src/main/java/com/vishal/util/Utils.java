package com.vishal.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;

import com.vishal.api.Segment;
import com.vishal.api.Stream;
import com.vishal.api.SystemParams;
import com.vishal.api.Task;

public class Utils {

   public static List<Segment> getInputSegments(String taskId, Stream input, int chunkSize) {
      List<Segment> segments = new ArrayList<Segment>();
      File f = new File(input.getFileName());
      int fileSize = (int) f.length();
      byte[] chunk = new byte[chunkSize];
      try (InputStream is = new FileInputStream(input.getFileName())) {
         int i = 0;
         int totalBytesRead = 0;
         while (totalBytesRead < fileSize) {
            int bytesRemaining = fileSize - totalBytesRead;
            if (bytesRemaining < chunkSize) {
               chunkSize = bytesRemaining;
            }
            chunk = new byte[chunkSize]; // Temporary Byte Array
            int bytesRead = is.read(chunk, 0, chunkSize);
            if (bytesRead > 0) {
               totalBytesRead += bytesRead;
            }
            String curChunk = new String(chunk);
            Segment seg = new Segment(input.getId());
            seg.setSize(bytesRead);
            seg.setId(input.getId() + "_" + "segment_" + taskId + "_" + i);
            seg.setOrigObj(curChunk);
            seg.setMd5Sum(getObjectMd5CheckSum(curChunk));
            segments.add(seg);
            i++;
         }
      } catch (IOException e) {
         e.printStackTrace();
         throw new RuntimeException("IOException occurred while reading file " + input.getFileName());
      }
      return segments;
   }

   public static void saveSegmentToFile(String file, Object obj) {
      File f = new File(SystemParams.OUTPUT_DIR_LOCATION);
      f.mkdirs();

      try (FileOutputStream fos = new FileOutputStream(file)) {
         String strObj = (String) obj;
         fos.write(strObj.getBytes());
      } catch (FileNotFoundException e) {
         e.printStackTrace();
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   public static void uploadSegmentsToFile(List<Segment> segments, String objKey, String outputFileName, Task task) {

      File f = new File(SystemParams.OUTPUT_DIR_LOCATION);
      f.mkdirs();
      try (FileOutputStream fos = new FileOutputStream(outputFileName)) {
         for (Segment segment : segments) {
            task.appendTaskDetail("copying " + segment.getFileName() + " to " + outputFileName);
            addSegmentToFileOutputStream(fos, segment, task);
         }
         for (Segment segment : segments) {
            Path path = Paths.get(segment.getFileName());
            Files.delete(path);
         }
      } catch (FileNotFoundException e) {
         e.printStackTrace();
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   public static void addSegmentToFileOutputStream(FileOutputStream fos, Segment segment, Task task) {
      File file = new File(segment.getFileName());
      try (FileInputStream is = new FileInputStream(file)) {
         StringBuilder builder = new StringBuilder();
         int ch;
         while ((ch = is.read()) != -1) {
            builder.append((char) ch);
         }
         fos.write(builder.toString().getBytes());
      } catch (FileNotFoundException e) {
         e.printStackTrace();
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   public static InputStream convertSegmentsToSingleStream(List<Segment> segments) {
      StringBuilder object = new StringBuilder();
      for (Segment segment : segments) {
         String origObjStr = (String) segment.getOrigObj();
         object.append(origObjStr);
      }
      return convertObject(object.toString());
   }

   public static InputStream convertObject(Object input) {

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
         oos.writeObject(input);
      } catch (IOException io) {
         io.printStackTrace();
      }
      InputStream is = new ByteArrayInputStream(baos.toByteArray());
      return is;
   }

   public static String getFileMd5CheckSum(String fileName) {
      String md5 = null;
      try (FileInputStream fis = new FileInputStream(fileName);) {
         md5 = DigestUtils.md5Hex(fis);
      } catch (IOException e) {
         e.printStackTrace();
      }
      return md5;
   }

   public static String getObjectMd5CheckSum(Object object) {

      InputStream stream = convertObject(object);
      return getStreamMd5CheckSum(stream);
   }

   public static String getStreamMd5CheckSum(InputStream stream) {
      String md5 = null;
      try {
         md5 = DigestUtils.md5Hex(stream);
      } catch (IOException e) {
         e.printStackTrace();
      }
      return md5;
   }

   public static int findPos(List<Segment> segments, Segment seg) {
      for (int i = 0; i < segments.size(); i++) {
         if (seg.equals(segments.get(i))) {
            return i;
         }
      }
      return -1;
   }
}
