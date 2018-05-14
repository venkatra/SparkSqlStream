**How to implement a custom spark sql streaming source, based of v2.3 ?**

Git Repo: [SparkSqlStream](https://github.com/venkatra/SparkSqlStream)

The "SparkSqlStream" is an example project meant to demonstrate as to how to implement custom spark sql structured streaming source. 
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
 

 _Note:_ 
 * These examples are not coded to be robust for production use-cases. They also do not cover all aspects of structured
 streaming (ex: recovery ,rollback). These, if needed, should be done by the implementor. 
 * This repo will not necessarily be maintained for future releases/versions of spark. Feel free to take it and 
 explore/enhance at your own will and needs.

***

# Implementing the source
## Source : RandomAlphaStreamSource
  The code for this is found in the implementation class _org.apache.spark.sql.execution.streaming.RandomAlphaStreamSource_. The "RandomAlphaStreamSource" is a source which would generate a stream which contains 
* timestamp : The time when the record was generated.
* value : A random alphanumeric character.

To start of ignore the fact that this class is an extension of _org.apache.spark.sql.execution.streaming.DontCareRecoveryStreamSourceBase_ (i just abstracted some of the basic code to this base class _DontCareRecoveryStreamSourceBase_).

 When implementing a Sql Stream source; you basically start of by extending _org.apache.spark.sql.execution.streaming.Source_ trait. The RandomAlphaStreamSource will be instantiated with the _SQLContext_ at run time. As an analogy, the source functions similar to Kafka (commit log) processing. 

The custom source would be queried, by the caller, to return message between a "start" and "end" offset. This means that there is no direct interaction between the caller and the stream itself. The custom source would thus have to have an internal buffer of message received from the stream or in other words a sort synchronized queue. messages should ideally be cleared from this buffer only when the "commit" method is called.

### Initialization
There is no concept of a "initialize" method in the base class; it is left upon the implementation specific. Hence we declare a "initialize" method and invoke it in the class.

![Initialize method call](wiki/images/initialize_method_call.png)

https://github.com/venkatra/SparkSqlStream/tree/master/wiki/images

In the initialization you can typically do the following
* Connect to the actual stream (ex: socket)
* Instantiate and initialize a holding buffer.
* Instantiate a thread which will listen to the stream and populate the buffer.
* Initialize the variable where offset is kept track off.

In the "RandomAlphaStreamSource" there is no specific resource/stream to connect too, however a generator thread "RandomStringGenerator" is created. This thread will execute a code that will generate a record and populate the internal buffer. To control the speed at which messages get added to the buffer; i have put a sleep of 2ms between new record generation and appending to the buffer. The code also updates the offset variable "currentOffset", which keeps track of the offset.

![Initialize method implementation](/images/initialize_method_impl.png)



