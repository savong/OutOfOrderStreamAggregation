/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.apache.flink.cmix;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;


/**
 *
 * @author savong.hashimoto_lpt
 */
public class CMiX_PWiX_MAX_using_Flink {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        

        
        DataStream<Tuple2<String,Double>> datastream = env.socketTextStream("localhost", 9999)
                .flatMap(new LineSplitter());
        
        
        int window = 32; // window size in second
        int slide = 2;  // slide size in second
        int max_lateness = 24; // maximum allowed lateness in second
        int p = (int) Math.ceil(max_lateness/slide); // number of slides in the maximum allowed lateness
        //partition the window by slide using Cutty approach
        int f2 = window%slide;//the remainer of the window/slide
        int f1 = slide - f2;
        if(f2==0)//If window size is dividiable by slide size
        {
            int n = (int) Math.ceil(window/slide);// number of partitions in the window
            int x = n-p;// The block size
            
            datastream
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator())
            .timeWindowAll(Time.seconds(window), Time.seconds(slide))//.sum(1).print();
            .evictor(TimeEvictor.of(Time.seconds(slide)))
            .reduce(new ReduceFunction<Tuple2<String,Double>>(){
               double[] cmix = new double[n];//cmix for keeping related aggregation of the current window
               double[] pwix = new double[p];//pwix for keeping the results of the past window
               Map<Integer, Double> pvalue_perslide = new HashMap<Integer, Double>();//for keeping the aggregation of the incoming records per slide based on the index (timestamp) in cmix

                boolean initialize=true;
                int round=0;
                int cycle_cpix=1;
                double Result =0;
                long start_time = Long.MAX_VALUE;
                long current_time;
                public Tuple2<String,Double> reduce(Tuple2<String,Double> v1, Tuple2<String,Double> v2)
                {
                    //Becase we compute maximum value, so the default values in both cmix and pwix must be set to DOUBLE.MIN_VALUE.
                    if(initialize)
                    {
                        for(int i=0;i<n;i++)
                        {
                            cmix[i] = Double.MIN_VALUE;
                        }
                        for(int i=0;i<p;i++)
                        {
                            pwix[i] = Double.MIN_VALUE;
                        }
                        initialize=false;
                    }
                    start_time = Math.min(start_time, Long.parseLong(v1.f0));
                    current_time = Math.max(current_time, Long.parseLong(v2.f0));
                    int duration = (int)((current_time - start_time)/1000);
                    int duration_current_record = (int)((Long.parseLong(v2.f0) - start_time)/1000);

                    //just ignore if the lateness is bigger than the maximum allowed lateness
                    if(current_time-Long.parseLong(v2.f0)<=max_lateness)
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
                                pvalue_perslide.put(index_falling, Math.max(pvalue_perslide.get(index_falling), v2.f1));
                            }
                            else
                            {
                                pvalue_perslide.put(index_falling, (double)v2.f1);
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
                            for (Map.Entry<Integer, Double> entry : pvalue_perslide.entrySet())
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
                                        cmix[(int)(Math.floor(round/x)*x)] = Math.max(cmix[(int)(Math.floor(round/x)*x)], entry.getValue());
                                    }
                                    //Aggregate into the 0 th partition if current slide is not pointing to the 0th index in CMiX
                                    //Line 10 in Algorithm 1
                                    if(entry.getKey()!=0)
                                    {
                                        cmix[0] = Math.max(cmix[0], entry.getValue());
                                    }
                                }
                                else//Line 11-21 in Algorithm 1
                                {
                                    //For each partition and aggregation pair in the set of late-arrival records
                                    //Aggregate into the affected partition by the late records
                                    //Line 12 in Algorithm 1
                                    cmix[entry.getKey()] = Math.max(cmix[entry.getKey()], entry.getValue());
                                    
                                    //Lines 13-15 in Algorithm 1
                                    //If affected index is not the left-most partition, aggregate to that partition in affected block
                                    if(entry.getKey() != (int)(Math.floor(entry.getKey()/x)*x) && (int)(Math.floor(entry.getKey()/x)) != k+1)
                                    {
                                        cmix[(int)(Math.floor(entry.getKey()/x)*x)] = Math.max(cmix[(int)(Math.floor(entry.getKey()/x)*x)], entry.getValue());
                                    }
                                }
                                
                                //Lines 16-20
                                //Aggregate to the left-most partition in the next_k block if the affected index is bigger than the current partition round or CMiX[0] otherwise
                                if(entry.getKey() > round)
                                {
                                    cmix[next_k*x] = Math.max(cmix[next_k*x], entry.getValue());
                                }
                                else
                                {
                                    cmix[0] = Math.max(cmix[0], entry.getValue());
                                }
                            }


                            //Line 22 in Algorithm 1
                            //Compute the query result using function “Compute_Result()”in Algorithm 2
                            Result = Compute_Result(cmix, Result, k,  next_k, round,  n, x);
                            System.out.println("At time: "+ current_time + ", Aggregating results:" + Result);

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
                                    cmix[next_k*x] = Math.max(cmix[next_k*x], cmix[j*x]);
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
                                pvalue_perslide.put(index_falling, Math.max(pvalue_perslide.get(index_falling), v2.f1));
                            }
                            else
                            {
                                pvalue_perslide.put(index_falling, (double)v2.f1);
                            }



                        }
                    }
                    
                    
                    return new Tuple2<>(v1.f0, v1.f1 + v2.f1);
                }
            }
            );//.print();
        }
        /*else
            {
                //If the window is not dividable by the slide.
                //Please refer to the pseudo code in Algorithm 6 in the paper for implementation.
                //We have not implemented this case yet, but do so soon.
            }*/
    

        // Execute program, beginning computation.
        env.execute("Flink Java API Skeleton");
    }
    
    //Algorithm 2 in the paper
    public static double Compute_Result(double[] cmix, double Result, int k, int next_k, int m, int n, int x)
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
                Result = Math.max(cmix[0], cmix[next_k*x]);
            }
            else//Otherwise (Lines 5-6 in Algorithm 2)
            {
                Result = Math.max(cmix[0], Math.max(cmix[next_m], cmix[next_k*x]));
            }
        }
        else//If the current block is the ⌈n/x⌉ − 1 th block (Line 8 in Algorithm 2):
        {
            if(m<n-1)// if the current partition is not the right-most one (Lines 9-10 in Algorithm 2)
            {
                Result = Math.max(cmix[0], cmix[next_m]);
            }
            else// Otherwise (Lines 11-12 in Algorithm 2)
            {
                Result = cmix[0];
            }
        }
        return Result;
    }
    
    //Algorithm 3 in the paper
    public static double[] Aggkplus1Block(double[] cmix, int k, int next_k, int m, int n, int x)
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
            cmix[next_k*x+i] = Math.max(cmix[next_k*x+i], cmix[next_k*x+i+1]);
        }
        return cmix;
    }

    //The aggregations of the current and past windows are maintained by Function “UpdatePCResult()”
    //Algorithm 4 in the paper
    public static double[] UpdatePCResult(double[] pwix, Map<Integer, Double> pvalue_perslide, int p, int m, double R, long start_time, int slide)
    {
        //When the window slides, the aggregation of the current window computed using the Algorithms 1 or 6 is kept in the partition of PWiX corresponding to the current slide (Line 2 in Algorithm 4).
        pwix[m%p] = R;
        
        //The affected partitions in PWiX by the late-arrival records are ordered into: (1) the right and (2) the left of the current partition using functions “OrderOn^r_p()” (Line 3) and “OrderOn^l_p()” (Line 4 in Algorithm 4) respectively.
        Four_values collection = sortbykey(pvalue_perslide, p, m);
        ArrayList<Integer> sortedKeys_l = collection.sortedKeys_l;
        ArrayList<Integer> sortedKeys_r = collection.sortedKeys_r;
        Map<Integer, Double> pvalue_perslide_pwix_l = collection.pvalue_perslide_pwix_l;
        Map<Integer, Double> pvalue_perslide_pwix_r = collection.pvalue_perslide_pwix_r;

        //The results of the affected past windows are updated by Function “UPResult” in Algorithm 5 in the paper.
        boolean have_right = false;
        double la_pre = 0;
        Pair_value collective_result;
        if(!pvalue_perslide_pwix_r.isEmpty())
        {
            collective_result = UPResult(pwix, pvalue_perslide_pwix_r, sortedKeys_r, la_pre, p-1, have_right, start_time, slide);
            pwix = collective_result.leftValue;
            la_pre = collective_result.rightValue;
            have_right = true;
        }
        if(!pvalue_perslide_pwix_l.isEmpty())
        {
            collective_result = UPResult(pwix, pvalue_perslide_pwix_l, sortedKeys_l, la_pre, m%p, have_right, start_time, slide);
            pwix = collective_result.leftValue;
            la_pre = collective_result.rightValue;
        }
        return pwix;
    }
    
    //Algorithm 5 in the paper
    //The affected partitions and other partitions with newer timestamps are updated by aggregating the late-arrival records into them (Lines 2–10 in Algorithm 5).
    public static Pair_value UPResult(double[] pwix, Map<Integer, Double> pvalue_perslide_in_pwix, ArrayList<Integer> sortedKeys, double la_pre, int lst, boolean have_right, long start_time, int slide)
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
                la_pre = Math.max(la_pre, pvalue_perslide_in_pwix.get(i));
            }
            pwix[i] = Math.max(pwix[i], la_pre);
            System.out.println("Past Result of the affected window computed at: "+start_time+slide*1000*i + ", Updated result: " + pwix[i] );
        }
        collective_result.leftValue = pwix;
        collective_result.rightValue = la_pre;
        return collective_result;
    }
    
    public static class Pair_value {
        double[] leftValue;
        double rightValue;
     }

    public static class Four_values {
        Map<Integer, Double> pvalue_perslide_pwix_l;
        Map<Integer, Double> pvalue_perslide_pwix_r;
        ArrayList<Integer> sortedKeys_l;
        ArrayList<Integer> sortedKeys_r;
     }

    
    public static Four_values sortbykey(Map<Integer, Double> pvalue_perslide, int p, int m)
    {
        Map<Integer, Double> pvalue_perslide_pwix_l = new HashMap<Integer, Double>();
        Map<Integer, Double> pvalue_perslide_pwix_r = new HashMap<Integer, Double>();
        for (Map.Entry<Integer, Double> entry : pvalue_perslide.entrySet())
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

    
    public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Double>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Double>> out) {
            String[] cells = line.split(",");
            //out.collect(new Tuple2<>(String.valueOf(System.currentTimeMillis()), Double.valueOf(cells[2])));
            out.collect(new Tuple2<>(cells[0], Double.valueOf(cells[1])));
        }
    }
                

    
    
    
    
public static class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<Tuple2<String, Double>> {

    private final long maxOutOfOrderness = 1; // 3.5 seconds

    private long currentMaxTimestamp;
    public long extractAscendingTimestamp(Tuple2<String, Double> element) {
        return Long.parseLong(element.f0);
    }

    @Override
    public long extractTimestamp(Tuple2<String, Double> element, long previousElementTimestamp) {
        long timestamp = Long.parseLong(element.f0);
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }


    @Override
    public Watermark getCurrentWatermark() {
        // return the watermark as current highest timestamp minus the out-of-orderness bound
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

}

}

