package amex.multithreaded;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Properties;

public class MainMultiThreaded {
    public static HashMap<Long, ArrayList<HashMap<String,Integer>>> finalHM = new HashMap<Long, ArrayList<HashMap<String,Integer>>>();

    /*
     * convert the output to desired format
     * i.e. <Long, HashMap<String, Integer>> to <Long, ArrayList<HashMap<String,Integer>>>
     * */
    public static void convertHashMapToArrayOfHashMaps(ConcurrentHashMap<Long, HashMap<String, Integer>> hm) {

        for (Long key : hm.keySet()) {
            ArrayList<HashMap<String, Integer>> arr = new ArrayList<HashMap<String, Integer>>();
            HashMap valHm = hm.get(key);
            for (Object k : valHm.keySet()) {
                HashMap<String, Integer> tmp = new HashMap<>();
                tmp.put((String) k, (int) valHm.get(k));
                arr.add(tmp);
            }

            finalHM.put(key, arr);
        }
        System.out.print("Final Formatted Result : "+ finalHM);
    }

    public static Properties readPropertiesFile(String fileName) throws IOException {
        FileInputStream fis = null;
        Properties prop = null;
        try {
            fis = new FileInputStream(fileName);
            prop = new Properties();
            prop.load(fis);
        } catch(FileNotFoundException fnfe) {
            fnfe.printStackTrace();
        } catch(IOException ioe) {
            ioe.printStackTrace();
        } finally {
            fis.close();
        }
        return prop;
    }


    public static void main(String[] args) throws IOException {

        /**
         * Read the numThreads.txt to get the threads allocated to run this program
         * Inputs required to run this Multi-Threaded program :
         * 1. Input File location (eg. "input/numThreads.txt") -passed as argument
         * 2. Number of Threads allocated to read the file
         * 3. Number of Lines processed each time by a thread
         * (For this problem, this is hardcoded, however in a production code, it will be determined depending upon multiple factors like Number of cores in the machine,
         * size of each line, total number of lines etc)
         * 4. hasHeader - indicates if file has a header -passed as argument
         */
        // reading application.properties file to get the numProperties
        Properties prop = readPropertiesFile("application.properties");

        // if number of arguments passed != 2, throw an exception
        if (args.length != 2){
            throw new IllegalArgumentException(" 2 arguments needs to be passed, 1. input file location 2. Boolean argument indicating  if file has a header");
        }

        long linesToProcessInEachThread = 10;
        Boolean hasHeader = true;
        if (args[1].equals("false")) {
            hasHeader = false;
        }

        String numThreadsStr = prop.getProperty("numThreads");
        if (numThreadsStr == null){
            //default to 1 thread, if numThreads is not specified in the application.properties file
            numThreadsStr = "1";
        }
        int numThreads = Integer.parseInt(numThreadsStr);

        /*
         * Using BufferredReader + FileReader to read file
         * */
        FileReader eReader = new FileReader(args[0]);
        BufferedReader br = new BufferedReader(eReader);

        String line;
        // if the files has header, the first line is skipped
        if (hasHeader){
            br.readLine();
        }

        // totalProcessed - keeps track of total number of rows processed by the threads
        long totalProcessed = 0;

        /**
         * leveraging ExecutorService to create a thread pool with size = numThreads
         * Each thread processes a fixed number of lines, and the process continues till all the times in the file are processed.
         * The lines are added to an ArrayList, and passed to the thread.
         */
        Boolean isDone = false;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        int threadNum = 0;
        while (!isDone){
            ArrayList<String> currList = new ArrayList<>();
            int currCnt = 0;
            while (currCnt < linesToProcessInEachThread)
            {
                line = br.readLine();
                if (line == null){
                    isDone = true;
                    break;
                }
                currList.add(line);
                currCnt++;
            }
            //pass arrayList to Thread
            if (currCnt > 0) {
                //arraylist containing lines to be processed is passed onto the Thread
                executor.execute(new RecordProcessorThread(currList, threadNum));
                if (isDone){
                    break;
                }
                threadNum = (threadNum + 1) % numThreads;
            }
            else{
                break;
            }
        }

        //Tear Down
        executor.shutdown();
        eReader.close();
        br.close();

        //Wait for all threads to finish
        while (!executor.isTerminated())
        {}//wait for infinity time
        System.out.println("Finished all threads");
//        System.out.println(" Final HashMap "+ Helper.hm);
        convertHashMapToArrayOfHashMaps(Helper.hm);

    }
}
