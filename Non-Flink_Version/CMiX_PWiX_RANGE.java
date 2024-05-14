import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.Serializable;
import static java.lang.Math.round;
import static java.lang.Math.sqrt;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/*
This is the implementation of the paper "O(1)-Time Complexity for Fixed Sliding-window Aggregation over Out-of-order Data Streams".
By Savong Bou, Toshiyuki Amagasa, Hiroyuki Kitagawa
*/

/**
 *
 * @author savong.hashimoto_lpt
 */

/*
RAANGE = MAXIMUM - MINIMUM.

As long as we can incrementally compute MAXIMUM and MINIMUM,
we can also incrementally compute RANGE.
*/
public class CMiX_PWiX_RANGE {
        public static void main(String[] args) throws UnknownHostException, IOException, ClassNotFoundException, InterruptedException{
        //get the localhost IP address, if server is running on some other IP, you need to use that
        InetAddress host = InetAddress.getLocalHost();
                
        Socket socket = new Socket(host.getHostName(), 9999);
        //Socket socket = new Socket(“127.0.0.1”, 5000) //if server is run on different machine

        
        BufferedReader in =
                new BufferedReader(
                        new InputStreamReader(socket.getInputStream()));
        String line = null;
        
        
        
        int window = 32; // window size in second
        int slide = 2;  // slide size in second
        int max_lateness = 24; // maximum allowed lateness in second
        int p = (int) Math.ceil(max_lateness/slide); // number of slides in the maximum allowed lateness
        //partition the window by slide using Cutty approach
        int f2 = window%slide; //the remainer of the window/slide
        int f1 = slide - f2;
        int n = (int) Math.ceil(window/slide); // number of partitions in the window
        int x = n-p; // The block size
        count_aggregation[] cmix = new count_aggregation[n]; //cmix for keeping related aggregation of the current window
        count_aggregation[] pwix = new count_aggregation[p]; //pwix for keeping the results of the past window
        //To avoid error when computing the result before the window is fully filled, set the default values in both cmix and pwix to 0
        for(int i=0;i<n;i++)
        {
            count_aggregation c_a= new count_aggregation();
            c_a.max = Double.MIN_VALUE;
            c_a.min = Double.MAX_VALUE;
            cmix[i] = c_a;
        }
        for(int i=0;i<p;i++)
        {
            count_aggregation c_a= new count_aggregation();
            c_a.max = Double.MIN_VALUE;
            c_a.min = Double.MAX_VALUE;
            pwix[i] = c_a;
        }
        Map<Integer, count_aggregation> pvalue_perslide = new HashMap<Integer, count_aggregation>(); //for keeping the aggregation of the incoming records per slide based on the index (timestamp) in cmix


        int round=0;
        int cycle_cpix=1;
        count_aggregation Result = new count_aggregation();
        long start_time = Long.MAX_VALUE;
        long current_time=0;
        //If window size is dividiable by slide size
        if(f2==0)
        {
            //Accepting incoming records from data streams
            while((line = in.readLine()) != null) {
                //This paper deals with out-of-order streams, so the progress of time depends on the event time.
                //Track the progress of time based on the timestamps of the incoming records from data streams
                String[] cells = line.split(",");
                start_time = Math.min(start_time, Long.parseLong(cells[0]));
                current_time = Math.max(current_time, Long.parseLong(cells[0]));
                int duration = (int)((current_time - start_time)/1000);
                int duration_current_record = (int)((Long.parseLong(cells[0]) - start_time)/1000);

                //just ignore if the lateness is bigger than the maximum allowed lateness
                if((current_time-Long.parseLong(cells[0]))/10000<=max_lateness)
                {
                    //Find the index of CMiX to which the incoming records fall.
                    //If the records are non-late, the corresponding index is current index. (index_falling = round)
                    int index_falling = (duration_current_record%window)/slide;
                    
                    //Begin-aggregate-perslide: aggregate the incoming records per slide based on the falling index.
                    //This is the same as obtaining (m, m_a), (q, q_a) inthe pseudo code in Line 4 in Algorithm 1 CMiX in the paper.
                    if(duration < (round+1)*f1)
                    {
                        if(pvalue_perslide.containsKey(index_falling))
                        {
                            count_aggregation co_ag_pair = pvalue_perslide.get(index_falling);
                            co_ag_pair.max = Math.max(co_ag_pair.max, Double.valueOf(cells[1]));
                            co_ag_pair.min = Math.min(co_ag_pair.min, Double.valueOf(cells[1]));
                            pvalue_perslide.put(index_falling, co_ag_pair);
                        }
                        else
                        {
                            count_aggregation co_ag_pair = new count_aggregation();
                            co_ag_pair.max = Double.valueOf(cells[1]);
                            co_ag_pair.min = Double.valueOf(cells[1]);
                            pvalue_perslide.put(index_falling, co_ag_pair);
                        }
                    }//End-aggregate-perslide
                    else
                    {
                        //After accepting all incoming records per slide
                        //k: is the current block (Line 5 in Algorithm 1)
                        int k = (int) Math.floor(round/x);
                        
                        //If k is not the right-most, next_k = k+1. Else k=0.
                        int next_k=0;
                        if(k == (int) Math.ceil(n/x)-1)
                        {
                            next_k = 0;
                        }
                        else
                        {
                            next_k = k+1;
                        }
                        
                        //loop over all aggregated records based on the falling index after one slide
                        //Lines 4-27 in Algorithm 1.
                        for (Map.Entry<Integer, count_aggregation> entry : pvalue_perslide.entrySet())
                        {
                            //Aggregate the non-late aggregation into CMiX
                            //Lines 6-10 in Algorithm 1
                            if(entry.getKey()==round)
                            {
                                //Update CMiX on the current slide
                                //Line 6 in Algorithm 1
                                cmix[entry.getKey()] = entry.getValue();
                                //Aggregate into the left-most partition in the current block if it is not left-most partition
                                //Lines 7-9
                                if(round != (int)(Math.floor(round/x)*x))
                                {
                                        count_aggregation co_ag_pair = cmix[(int)(Math.floor(round/x)*x)];
                                        co_ag_pair.max = Math.max(co_ag_pair.max, entry.getValue().max);
                                        co_ag_pair.min = Math.min(co_ag_pair.min, entry.getValue().min);
                                        cmix[(int)(Math.floor(round/x)*x)] = co_ag_pair;
                                }
                                //Aggregate into the 0 th partition if current slide is not pointing to the 0th index in CMiX
                                //Line 10 in Algorithm 1
                                if(entry.getKey()!=0)
                                {
                                    count_aggregation co_ag_pair = cmix[0];
                                    co_ag_pair.max = Math.max(co_ag_pair.max, entry.getValue().max);
                                    co_ag_pair.min = Math.min(co_ag_pair.min, entry.getValue().min);
                                    cmix[0] = co_ag_pair;
                                }
                            }
                            else //Line 11-21 in Algorithm 1
                            {
                                //For each partition and aggregation pair in the set of late-arrival records
                                //Aggregate into the affected partition by the late records
                                //Line 12 in Algorithm 1
                                count_aggregation co_ag_pair = cmix[entry.getKey()];
                                co_ag_pair.max = Math.max(co_ag_pair.max, entry.getValue().max);
                                co_ag_pair.min = Math.min(co_ag_pair.min, entry.getValue().min);
                                cmix[entry.getKey()] = co_ag_pair;
                                
                                //Lines 13-15 in Algorithm 1
                                //If affected index is not the left-most partition, aggregate to that partition in affected block
                                if(entry.getKey() != (int)(Math.floor(entry.getKey()/x)*x) && (int)(Math.floor(entry.getKey()/x)) != k+1)
                                {
                                    count_aggregation co_ag_pair1 = cmix[(int)(Math.floor(entry.getKey()/x)*x)];
                                    co_ag_pair1.max = Math.max(co_ag_pair1.max, entry.getValue().max);
                                    co_ag_pair1.min = Math.min(co_ag_pair1.min, entry.getValue().min);
                                    cmix[(int)(Math.floor(entry.getKey()/x)*x)] = co_ag_pair1;
                                }
                            }
                            
                            //Lines 16-20
                            //Aggregate to the left-most partition in the next_k block if the affected index is bigger than the current partition round or CMiX[0] otherwise
                            if(entry.getKey() > round)
                            {
                                count_aggregation co_ag_pair = cmix[next_k*x];
                                co_ag_pair.max = Math.max(co_ag_pair.max, entry.getValue().max);
                                co_ag_pair.min = Math.min(co_ag_pair.min, entry.getValue().min);
                                cmix[next_k*x] = co_ag_pair;
                            }
                            else
                            {
                                count_aggregation co_ag_pair = cmix[0];
                                co_ag_pair.max = Math.max(co_ag_pair.max, entry.getValue().max);
                                co_ag_pair.min = Math.min(co_ag_pair.min, entry.getValue().min);
                                cmix[0] = co_ag_pair;
                            }
                        }
                        //Line 22 in Algorithm 1
                        //Compute the query result using function “Compute_Result()”in Algorithm 2
                        Result = Compute_Result(cmix, Result, k,  next_k, round,  n, x);
                        System.out.println("At time: "+ current_time + ", Aggregating results:" + (Result.max-Result.min));
                        
                        //If current slide is the right-most partition in any block
                        //Line 23
                        if(round%x == x-1 || round == n-1)
                        {
                            //Compute the aggregation in the next_k th block backwardly
                            //The following block of codes is the same as the function “Aggregatekplus1Block()” in Algorithm 3 in the paper.
                            cmix = Aggkplus1Block(cmix, k, next_k, round, n, x);
                            
                            //Update the left-most partition in the next_k+1 th block by the left-most partitions in all blocks on its right-handside (Line 25 in Algorithm 1).
                            for(int j= next_k+1;j<(int) Math.ceil(n/x);j++)
                            {
                                count_aggregation co_ag_pair = cmix[next_k*x];
                                co_ag_pair.max = Math.max(co_ag_pair.max, cmix[j*x].max);
                                co_ag_pair.min = Math.min(co_ag_pair.min, cmix[j*x].min);
                                cmix[next_k*x] = co_ag_pair;
                            }
                        }
                        
                        
                        //This function updates the results of the past windows affected by the late-arrival records.
                        //Same as Algorithm 4 in the paper
                        pwix = UpdatePCResult(pwix, pvalue_perslide, p, round, Result, start_time, slide);

                        if(round == n-1)
                        {
                            round = 0;
                            start_time = start_time + cycle_cpix*window*1000;
                            cycle_cpix++;
                        }
                        else
                        {
                            round++;
                        }
                        

                        pvalue_perslide.clear();
                        
                        //Aggregate the incoming records per slide based on the falling index.
                        //This is the same as obtaining (m, m_a), (q, q_a) inthe pseudo code in Line 4 in Algorithm 1 CMiX in the paper.
                        if(pvalue_perslide.containsKey(index_falling))
                        {
                            count_aggregation co_ag_pair = pvalue_perslide.get(index_falling);
                            co_ag_pair.max = Math.max(co_ag_pair.max, Double.valueOf(cells[1]));
                            co_ag_pair.min = Math.min(co_ag_pair.min, Double.valueOf(cells[1]));
                            pvalue_perslide.put(index_falling, co_ag_pair);
                        }
                        else
                        {
                            count_aggregation co_ag_pair = new count_aggregation();
                            co_ag_pair.max = Double.valueOf(cells[1]);
                            co_ag_pair.min = Double.valueOf(cells[1]);
                            pvalue_perslide.put(index_falling, co_ag_pair);
                        }
                    }
                    
                    
                }
            }
            in.close();
            socket.close();
        }
        /*else
            {
                //If the window is not dividable by the slide.
                //Please refer to the pseudo code in Algorithm 6 in the paper for implementation.
                //We have not implemented this case yet, but do so soon.
            }*/
    }
    
        
    //Algorithm 2 in the paper
    public static count_aggregation Compute_Result(count_aggregation[] cmix, count_aggregation Result, int k, int next_k, int m, int n, int x)
    {
        int next_m = 0;
        if(m == n-1)
        {
            next_m = 0;
        }
        else
        {
            next_m = m+1;
        }

        //If the current block is not the ⌈n/x⌉ − 1 th block (Line2 in Algorithm 2)
        if(k < Math.ceil(n/x)-1)
        {
            if(m%x == x-1)//if the current partition is the right-most one (Lines 3-4 in Algorithm 2)
            {
                Result.max = Math.max(cmix[0].max, cmix[next_k*x].max);
                Result.min = Math.min(cmix[0].min, cmix[next_k*x].min);
            }
            else//Otherwise (Lines 5-6 in Algorithm 2)
            {
                Result.max = Math.max(cmix[0].max, Math.max(cmix[next_m].max, cmix[next_k*x].max));
                Result.min = Math.min(cmix[0].min, Math.min(cmix[next_m].min, cmix[next_k*x].min));
            }
        }
        else//If the current block is the ⌈n/x⌉ − 1 th block (Line 8 in Algorithm 2):
        {
            if(m<n-1)// if the current partition is not the right-most one (Lines 9-10 in Algorithm 2)
            {
                Result.max = Math.max(cmix[0].max, cmix[next_m].max);
                Result.min = Math.min(cmix[0].min, cmix[next_m].min);
            }
            else// Otherwise (Lines 11-12 in Algorithm 2)
            {
                Result.max = cmix[0].max;
                Result.min = cmix[0].min;
            }
        }
        return Result;
    }
    
    //Algorithm 3 in the paper
    public static count_aggregation[] Aggkplus1Block(count_aggregation[] cmix, int k, int next_k, int m, int n, int x)
    {
        //The size of the right-most block is n%x − 1 if n%x ̸= 0. Otherwise, its size is x−1. The size of other remaining blocks is x−1 (Lines 2–6 in Algorithm 3).
        int block = 0;
        if(n%x!=0 && m==n-n%x-1)
        {
            block = n%x-1;
        }
        else
        {
            block = x-1;
        }
        
        // The aggregation is done backwardly from the right-most to the left-most partitions in the next block (k)(Lines 12–14 in Algorithm 3)
        for(int i = block-2; i>=0; i--)
        {
            count_aggregation co_ag_pair = cmix[next_k*x+i];
            co_ag_pair.max = Math.max(co_ag_pair.max, cmix[next_k*x+i+1].max);
            co_ag_pair.min = Math.min(co_ag_pair.min, cmix[next_k*x+i+1].min);
            cmix[next_k*x+i] = co_ag_pair;
        }
        return cmix;
    }

    //The aggregations of the current and past windows are maintained by Function “UpdatePCResult()”
    //Algorithm 4 in the paper
    public static count_aggregation[] UpdatePCResult(count_aggregation[] pwix, Map<Integer, count_aggregation> pvalue_perslide, int p, int m, count_aggregation R, long start_time, int slide)
    {
        //When the window slides, the aggregation of the current window computed using the Algorithms 1 or 6 is kept in the partition of PWiX corresponding to the current slide (Line 2 in Algorithm 4).
        pwix[m%p] = R;
        
        //The affected partitions in PWiX by the late-arrival records are ordered into: (1) the right and (2) the left of the current partition using functions “OrderOn^r_p()” (Line 3) and “OrderOn^l_p()” (Line 4 in Algorithm 4) respectively.
        Four_values collection = sortbykey(pvalue_perslide, p, m);
        ArrayList<Integer> sortedKeys_l = collection.sortedKeys_l;
        ArrayList<Integer> sortedKeys_r = collection.sortedKeys_r;
        Map<Integer, count_aggregation> pvalue_perslide_pwix_l = collection.pvalue_perslide_pwix_l;
        Map<Integer, count_aggregation> pvalue_perslide_pwix_r = collection.pvalue_perslide_pwix_r;

        //The results of the affected past windows are updated by Function “UPResult” in Algorithm 5 in the paper.
        boolean have_right = false;
        double la_pre_max = Double.MIN_VALUE;
        double la_pre_min = Double.MAX_VALUE;
        Pair_value collective_result;
        if(!pvalue_perslide_pwix_r.isEmpty())
        {
            collective_result = UPResult(pwix, pvalue_perslide_pwix_r, sortedKeys_r, la_pre_max, la_pre_min, p-1, have_right, start_time, slide);
            pwix = collective_result.pwix;
            la_pre_max = collective_result.la_pre_max;
            la_pre_min = collective_result.la_pre_min;
            have_right = true;
        }
        if(!pvalue_perslide_pwix_l.isEmpty())
        {
            collective_result = UPResult(pwix, pvalue_perslide_pwix_l, sortedKeys_l, la_pre_max, la_pre_min, m%p, have_right, start_time, slide);
            pwix = collective_result.pwix;
            //la_pre = collective_result.la_pre;
            //la_pre_count = collective_result.la_pre_count;
        }
        return pwix;
    }
    
    //Algorithm 5 in the paper
    //The affected partitions and other partitions with newer timestamps are updated by aggregating the late-arrival records into them (Lines 2–10 in Algorithm 5).
    public static Pair_value UPResult(count_aggregation[] pwix, Map<Integer, count_aggregation> pvalue_perslide_in_pwix, ArrayList<Integer> sortedKeys, double la_pre_max, double la_pre_min, int lst, boolean have_right, long start_time, int slide)
    {
        Pair_value collective_result = new Pair_value();
        int l = sortedKeys.get(0);
        if(have_right)
        {
            l = 0;
        }
        
        for(int i=l; i<lst; i++)
        {
            if(sortedKeys.contains(i))
            {
                la_pre_max = Math.max(la_pre_max, pvalue_perslide_in_pwix.get(i).max);
                la_pre_min = Math.min(la_pre_min, pvalue_perslide_in_pwix.get(i).min);
            }
            pwix[i].max = Math.max(pwix[i].max, la_pre_max);
            pwix[i].min = Math.min(pwix[i].min, la_pre_min);
            System.out.println("Past Result of the affected window computed at: "+start_time+slide*1000*i + ", Updated result: " + (pwix[i].max-pwix[i].min) );
        }
        collective_result.pwix = pwix;
        collective_result.la_pre_max = la_pre_max;
        collective_result.la_pre_min = la_pre_min;
        return collective_result;
    }
    
    public static class Pair_value {
        count_aggregation[] pwix;
        double la_pre_max;
        double la_pre_min;
     }

    public static class Four_values {
        Map<Integer, count_aggregation> pvalue_perslide_pwix_l;
        Map<Integer, count_aggregation> pvalue_perslide_pwix_r;
        ArrayList<Integer> sortedKeys_l;
        ArrayList<Integer> sortedKeys_r;
     }

    public static class count_aggregation implements Serializable{
        double min = 0.0;
        double max = 0.0;
     }

    
    public static Four_values sortbykey(Map<Integer, count_aggregation> pvalue_perslide, int p, int m)
    {
        Map<Integer, count_aggregation> pvalue_perslide_pwix_l = new HashMap<Integer, count_aggregation>();
        Map<Integer, count_aggregation> pvalue_perslide_pwix_r = new HashMap<Integer, count_aggregation>();
        for (Map.Entry<Integer, count_aggregation> entry : pvalue_perslide.entrySet())
        {
            if(entry.getKey()%p<=m%p)
            {
                pvalue_perslide_pwix_l.put(entry.getKey()%p, entry.getValue());
            }
            else
            {
                pvalue_perslide_pwix_r.put(entry.getKey()%p, entry.getValue());
            }
        }
        ArrayList<Integer> sortedKeys_l
            = new ArrayList<Integer>(pvalue_perslide_pwix_l.keySet());
        ArrayList<Integer> sortedKeys_r
            = new ArrayList<Integer>(pvalue_perslide_pwix_r.keySet());
 
        Collections.sort(sortedKeys_l);
        Collections.sort(sortedKeys_r);
        Four_values four_collective = new Four_values();
        four_collective.pvalue_perslide_pwix_l = pvalue_perslide_pwix_l;
        four_collective.pvalue_perslide_pwix_r = pvalue_perslide_pwix_r;
        four_collective.sortedKeys_l = sortedKeys_l;
        four_collective.sortedKeys_r = sortedKeys_r;
        return four_collective;
    }
}
