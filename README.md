# Advanced Systems Lab (ETH)
---

The source code are inside `asl/src` directory. Run a single command ``ant`` inside `asl` directory to compile the source code. The jar file will be available inside `asl/dist` directory. 

There are four source files inside the `asl/src` directory:

* **mcRequest.java**: This class represents a memcache request which holds the socket channel from where request came and all timestamps used for instrumentation purpose, including the set of functions that were used to read/write the values to variable.  
* **netThread.java**: This class is responsible mainly for five top-level functions such as listening for new connections, reading the request sent by client, constructing an object for class named 'mcRequest.java', putting that object into the request queue and creating worker threads. 
* **RunMW.java**: This is the basic file provided by course assistants, which is reponsible to parse the arguments passed to middleware. This file has been further tweaked to catch kill command and to write the the recorded response time of each request to a log file named 'HistResponseTime.log'. 
* **workerThread.java**: This is the class representation of worker thread, while is responsible for most of the work in middleware such as parsing and processing the request, starting the logger, etc. 

**_Note: Each of the source files has inline comments which describes the file structure, parameters, functions in more detail._**