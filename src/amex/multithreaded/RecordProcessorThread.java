package amex.multithreaded;

import java.util.ArrayList;

public class RecordProcessorThread implements Runnable{


    private ArrayList _arr;
    private int _thread;

    public RecordProcessorThread(ArrayList<String> arr, int thread) {
        _arr = arr;
        _thread = thread;
    }


    @Override
    public void run() {

        try {
            int lenArr = _arr.size();
            Helper help = new Helper();
            for (int j=0;j<lenArr; j++){
                // spliu the string, sort the string, update the hashmap
                String line = (String)_arr.get(j);
                String[] arr = line.split(",");
                if (arr.length != 3){
                    throw new IllegalArgumentException(" Incorrect input, NUmber of splits should be 3 ") ;
                }

                Long datehour = help.GetDateHourFromEpoch(arr[0]);
                String anagram = help.returnSortedString(arr[1]);
                Helper.updateHashMap(anagram, Integer.parseInt(arr[2]), datehour);
//                System.out.print(" HelperClass.updateHashMap "+ HelperClass.hm);

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
