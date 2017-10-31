ClientApplicationDesignDoc.dox - Brief design of how this was implemented and how the System is supposed to work.

Pre-requsites:
  Jre 7 should be installed to run the applicaiton
  Jdk 1.7 should be installed and Eclipse with maven plugin or STS to launch the source code in Eclipse


Vishal.txt
  a. Please download the file and change the extension to rar.
  b. Unrar the Vishal.rar file
  c. A Vishal folder should get created which should contain a oracle folder and a jar file .\Oracle-1.0-SNAPSHOT-spring-boot.jar

Steps to run the application:

  target folder contains the jar file Oracle-1.0-SNAPSHOT-spring-boot.jar which could be used to run the program.
     i.  Please open a command prompt and navigate to the folder .\Vishal
     ii. Run the program using the command
                 java -jar .\Oracle-1.0-SNAPSHOT-spring-boot.jar

Steps to import Source code to Eclipse and run the application from Eclipse
 
  Open Eclipse
    i. Click on File > Import > Maven >  Existing Maven Projects
   ii. Select Root directory as Vishal\oracle folder created in above step
  iii. Click Finish
  iv. Run the File ClientApplication.java

To view the code in Windows
  source code is located in the following folder
  .vishal\oracle\src\main\java\com\vishal

Code flow
==========

Client launches Client Application inside Client Pakcage. 
Client will provide inputs to perform list of PUT/GET/DELETE operations
ClientImpl will invoke Workflow APIs to perform the requested Service. Workflow will create WfEngineRunner Threads. These Threads are managed by ServiceThreadPoolExecutor.
WfEngineRunner Thread will invoke WorkflowRunner API to perform actual PUT/GET operations.
  WorkflowRunne.executePUT - Will Segment the large Object and creates Segment Threads to perform PUT of each Segment.
  WorkflowRunne.executeGET - Will create required number of Segment threads to perform GET of each Segment and then assembles back all the Segments to form the Original Large Object
  Both the above Threads will use OracleStorageConnectionMgr to get Conneciton to OracleStorageCloud Service and use OracleStorageConneciton to communicate with Oracle Cloud Storage Service
and perform upload/download/delete operations.


Links to download 
=================

https://maven.apache.org/download.cgi
https://eclipse.org/downloads/
http://www.oracle.com/technetwork/java/javase/downloads/jre7-downloads-1880261.html
https://spring.io/tools/sts/all
