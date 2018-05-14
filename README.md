# SparkSqlStream

 This is an example project meant to demonstrate as to how to implement custom spark sql structured streaming source. 
 This is based of the implementation code from  
 [spark v2.3](https://github.com/apache/spark/tree/v2.3.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming). 
 
 The existing dstream based streaming source documentation exists here 
 [Using the custom receiver in a Spark Streaming application](https://spark.apache.org/docs/latest/streaming-custom-receivers.html).
 However this implementation is not same for structured sql streaming. As of this writing; i was not able to find a 
 clear cut documentation on how to implement a source for structured streaming. The only clue/lead I had to start with 
 was from the stackoverflow :
 
 [How to create a custom streaming data source?](https://stackoverflow.com/questions/47604184/how-to-create-a-custom-streaming-data-source)
 
 This example project is my take on deciphering the working of the source [TextSocketSource](https://github.com/apache/spark/blob/v2.3.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/socket.scala)
 using which I have implemented the following streaming sources
 * __**RandomAlphaStreamSource**__ : This generates a random set of alpha numeric characters and send the same via the stream. 
 This is a stripped down to the core implementation similar to "rate source".
 * __**MeetupRSVPStreamSource**__ : This is a simple implementation of the real time stream from [Meetup RSVP](https://www.meetup.com/meetup_api/docs/2/rsvps)
 .
 
 _Note:_ 
 * These examples are not coded to be robust for production use-cases. They also do not cover all aspects of structured
 streaming (ex: recovery ,rollback). These, if needed, should be done by the implementor. 
 * This repo will not necessarily be maintained for future releases/versions of spark. Feel free to take it and 
 explore/enhance at your own will and needs.
 
The theory and implementation logic is explained in the page [wiki/Home.md](./wiki/Home.md).

***

__Build__
```commandline
 mvn compile package
```
 Should produce the packaged shaded jar at: target/scala-2.11/jars/SparkSqlStream-1.0-SNAPSHOT.jar
 
__Run__

I have created an invocation script __bin/SubmitStreamApp.sh__.
```commandline
 bin/SubmitStreamApp.sh
``` 
You might need to modify all the environment related variables such as SPARK_HOME as per your environment.

The 

 