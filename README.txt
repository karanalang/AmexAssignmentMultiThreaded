Project : AmexAssignment
Solution 1 : Single Threaded approach

Description :
In this approach, the program reads the input file line-by-line, parses each line to get the a)datehour b)sortedString c)aggregated count.
The aggregated count for the sortedString is updated in the HashMap.
Finally, the HashMap contents are re-arranged to get the final output, in the required format.

Classes Used :

MainSingleThreaded.java
- This is the driver program which creates an Object of the ProcessFile class, passing 2 inputs
1. input file location
2. Boolean variable specifying if the file has header or not, if the file has a header the first line is skipped.
Expected File format - csv with columns epochMillis, word, count

The above 2 need to be passed as arguments to the main() function

FileProcessor.java
- Has the following methods, used to perform the count aggregation of the Anagram words.
1. public String returnSortedString(String s) - returns the sorted string, the count of which is stored in the HashMap
2. public void updateHashMap(String anagram, int cnt, Long time) - updates the HashMap, with key as dateHr and value as HashMap containing the anagram and count
3. public Long GetDateHourFromEpoch(String epoch) - converts the epoch timestamp to dateHr in log datatype, this is used as key in the HashMap
4. public void convertHashMapToArrayOfHashMaps() - converts the result HashMap which has the format <Long, HashMap<String, Integer>> into required format <Long, ArrayList<HashMap<String, Integer>>>
5. public void processFile(String input , Boolean hasHeader) - this is called from the Main.java file and internally calls the other functions in ProcessFile.java

Project : AmexAssignmentMultiThreaded
Solution 2 : Multi-Threaded Approach

Description. Merits/De-merits :
In this approach, multiple threads are used to do the aggregation, and getting the final output.
The approach shows an alternate methodology wherein the multi-threaded approach can be leveraged to improve the performance of the application by a magnitude,
especially if the thread is required to do a lot of heavy-lifting.


Classes Used :

MainMultiThreaded.java
- Starting point of the program
- Creates the ExecutorService and ThreadPool
- parses the file and passes the records to the Thread for processing
- uses the following inputs
    1. file path
    2. Boolean variable specifying if the file has header or not, if the file has a header the first line is skipped.
    3. Total number of lines in the File
    4. Number of Threads allocated to read the file
    5. Number of Lines processed each time by a thread
    Pls note :
        For this problem, Max Number of lines processed each time by a thread is hardcoded, however in a production code it will
        be determined depending upon multiple factors like Number of cores in the machine, size of each line, total number of lines in the file, Memory allocated etc.
    6. hasHeader - indicates if file has a header
- Displays the final output on the Console.

RecordProcessorThread.java
- Thread class where the run() method is implemented
- run() methods does the following:
    - uses the methods in the Helper.java to parse each line
    - aggregate the count of the anagrams words in each dateHr
    - Update the ConcurrentHashMap

Helper.java
- contains the following Helper methods used to implement the aggregation logic for Anagarm words
    1. public String returnSortedString(String s) - returns the sorted string, the count of which is stored in the HashMap
    2. public void updateHashMap(String anagram, int cnt, Long time) - updates the ConcurrentHashMap, with key as dateHr and value as HashMap containing the anagram and count
    3. public Long GetDateHourFromEpoch(String epoch) - converts the epoch timestamp to dateHr in log datatype, this is used as key in the HashMap


Solution 3 (not coded): Model this as a Publisher-Subscriber.
In this approach, the Producer will read the data from the file and put it in the queue, while the Consumer will read the data, and update the HashMap.


Instructions to run the project:

1. create a jar file
sample command : jar cf amex.jar <project location>

2. run the jar file, passing the inputFileName and hasHeader as arguments
sample command : java -jat amex.jar input/input.csv false

Pls note :
The final output is displayed on the console