package amex.multithreaded;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class Helper {

    /*
     * CurrentHashMap is used in this approach, since it is thread-safe
     * Also, it is more optimized than a SynchronizedMap. since it uses locking at the key level
     * */
    public static ConcurrentHashMap<Long, HashMap<String, Integer>> hm = new ConcurrentHashMap<Long, HashMap<String, Integer>>();

    /*
     * Returns sorted string,the count fpr each sorted string is stored in HashMap
     * */
    public String returnSortedString(String s){
        char[] arr = s.toCharArray();
        Arrays.sort(arr);
        return new String(arr);
    }

    /*
     * Helper method to update the ConcurrentHashMap
     * */

    public static void updateHashMap(String anagram, int cnt, Long time){

        if (hm.containsKey(time)){
            HashMap<String, Integer> hmv = hm.get(time);
            hmv.put(anagram, hmv.getOrDefault(anagram,0)+cnt);
        }else{
            HashMap<String, Integer> hmv = new HashMap<String, Integer>();
            hmv.put(anagram, cnt);
            hm.put(time, hmv);
        }
    }

    /*
     * Helper method to get the dateHr from the epochTimeMillis
     * */
    public Long GetDateHourFromEpoch(String epoch){
        Long millis = Long.parseLong(epoch);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhh");
        Long dateLong=Long.parseLong(sdf.format(millis));
        return dateLong;
    }
}
