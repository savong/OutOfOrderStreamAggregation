package non.fifo.aggregation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import static java.lang.Math.ceil;
import static java.lang.Math.round;
import static java.lang.Math.sqrt;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Stack;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.util.Collector;


public class CMiX_PWiX {
    public static void MAX(){
        // TODO code application logic here
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(1);
        
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        
      
        
        //DataStream<Tuple2<Integer,Integer>> datastream = env.readTextFile("/DEBS2012_Dataset/DEBS2012_129millions.txt")//DEB12/DEBS2012-ChallengeData.txt") //5000000.txt //50000.txt
        //.flatMap(new LineSplitter2());
        
        DataStream<Tuple2<String,Integer>> savong1 = env.socketTextStream("localhost", 9090)
                .flatMap(new NonFifoAggregation.LineSplitter());
        
        
        long tStart= System.currentTimeMillis();//=0;// = System.currentTimeMillis();
        long tEnd=0;
        Double elapsedSeconds=0.0;

        int range = 8; // window size
                int slide = 2;
                int f2 = range%slide;
                int f1 = slide - f2;
                if(f2==0)
                {
                int num_slide = range/slide;
                int n_checkpoints = (int)round(sqrt(num_slide));
                int checkpoint_size = num_slide/n_checkpoints;
                int rmd = num_slide%n_checkpoints;
                

                //datastream
                savong1
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator())
                .timeWindowAll(Time.seconds(range), Time.seconds(slide))
                //.trigger(PurgingTrigger.of(ContinuousEventTimeTrigger.of(Time.milliseconds(slide))))
                //.evictor(TimeEvictor.of(Time.milliseconds(slide)))
                .evictor(TimeEvictor.of(Time.seconds(slide)))
                //savong
                //.keyBy(0)
                //.countWindow(range,slide).trigger(PurgingTrigger.of(CountTrigger.of(slide)))//.evictor(CountEvictor.of(slide))
                .fold(new Integer (0), new FoldFunction<Tuple2<String, Integer>, Integer>() {
                   int round=0;
                   int chkp_count=0;
                   int chkp_round=0;
                   int rmd_tmp = rmd;
                   double g_value=0;
                   boolean full_window=false;
                   Node Tree = BST(checkpoint_size+rmd_tmp);
                   double[] cvalue = new double[n_checkpoints];
                   double[] pvalue = new double[num_slide];
                   Map<Integer, Double> pvalue_perslide = new HashMap<Integer, Double>();
                   int currTimestamp=0;
                   int currTimes_lastslide=-1;
                   int currTimes_lastchpoint=0;
                   //double
                   @Override
                   public Integer fold(Integer acc, Tuple2<String, Integer> value) throws Exception{
                        //if(count<=f1)
                        if(Integer.parseInt(value.f0)/1000 <= currTimes_lastslide + f1)
                        {
                            //currTimestamp = Math.max(currTimestamp, Integer.parseInt(value.f0));//millisecond
                            currTimestamp = Math.max(currTimestamp, Integer.parseInt(value.f0)/1000);//second
                            for(int k=0;k<num_slide;k++)
                            {
                                //if(inRange(slide*k, slide*(k+1), (Integer.parseInt(value.f0))%(range))) //millisecond
                                if(inRange(slide*k, slide*(k+1), (Integer.parseInt(value.f0)/1000)%(range))) //second
                                {
                                    //pvalue_perslide.put(k, (double)value.f1);
                                    if(pvalue_perslide.containsKey(k))
                                        pvalue_perslide.put(k, Math.max(pvalue_perslide.get(k), value.f1));
                                    else
                                        pvalue_perslide.put(k, (double)value.f1);
                                    break;
                                }
                            }
                        }

                        //if(round_tmp%slide==0)
                        //if(round_tmp>=slide)
                        if(Integer.parseInt(value.f0)/1000 >= currTimes_lastslide + slide)
                        {
                            chkp_count++;
                            for (Map.Entry<Integer, Double> entry : pvalue_perslide.entrySet())
                            {
                                //if late arrival records fall onto the respecitve pvalue and cvalue.
                                int low_bound = 0;
                                for(int j=0;j<n_checkpoints;j++)
                                {
                                    if(inRange(low_bound, checkpoint_size*(j+1)+rmd, entry.getKey()))
                                    {
                                        //cvalue[j]+=entry.getValue();
                                        cvalue[j] = Math.max(cvalue[j], entry.getValue());
                                        break;
                                    }
                                    low_bound = checkpoint_size*(j+1)+rmd;
                                }
                                pvalue[entry.getKey()] = entry.getValue();
                                //g_value += entry.getValue();
                                g_value = Math.max(g_value, entry.getValue());
                                //if late arrival records fall onto the tree
                                //insert(Tree, entry.getKey()+1, entry.getValue(), 1);
                            }
                            pvalue_perslide.clear();
                        //    cvalue[chkp_round]+=f1_value;
                        //    pvalue[round] = f1_value;
                            insert(Tree, chkp_count, 0, 1);
                            double results=0.0;
                            if(chkp_count < checkpoint_size+rmd_tmp)
                                //results = g_value+Tree.aggregating;
                                results = Math.max(g_value, Tree.aggregating);
                            else
                                results = g_value;

                            if(chkp_count >= checkpoint_size+rmd_tmp)
                            //if(Integer.parseInt(value.f0)/1000 >= currTimes_lastchpoint + checkpoint_size + rmd_tmp)
                            {
                                chkp_count=0;
                                chkp_round++;
                                rmd_tmp=0;
                                if(chkp_round >= n_checkpoints)
                                {
                                    rmd_tmp = rmd;
                                    chkp_round=0;
                                }
                                if(full_window==true || round==num_slide-1)
                                {
                                   Tree = BST(checkpoint_size+rmd_tmp);
                                   //insert(Tree, round, 0, 1);
                                   int tmp_curptr=0;
                                   if(round==num_slide-1)
                                       tmp_curptr = 0;
                                   else
                                       tmp_curptr=round+1;
                                   for(int k=0;k<checkpoint_size+rmd_tmp;k++)
                                   {
                                       //pvalue[k+tmp_curptr] += pvalue[k+1+tmp_curptr];
                                       insert(Tree, k+1, pvalue[k+tmp_curptr], 1);
                                   }
                                   //insert(Tree, 2, 0, 1);
                                   g_value=0;
                                   cvalue[chkp_round]=0;
                                   for(int k=0;k<n_checkpoints;k++)
                                   {
                                       //g_value += cvalue[k];
                                       g_value = Math.max(g_value, cvalue[k]);
                                   }
                                }
                                currTimes_lastchpoint = currTimestamp;
                            }
                            round++;
                            if(round==num_slide)
                            {
                                full_window=true;
                                round=0;
                            }
                               
                            //Integer result=0;//(int)Tree.aggregating;
                            acc = (int)results;
                            currTimes_lastslide = currTimestamp;
                        }
                        return acc;
                   }
                }).print();//.writeAsText("/Users/bousavong/Desktop/test/text/DEB12/bidirection.txt");//.print();
                }
                else
                {
                int num_slide = range/slide+1;
                int n_checkpoints = (int)round(sqrt(num_slide));
                int checkpoint_size = num_slide/n_checkpoints;
                int rmd = num_slide%n_checkpoints;
                
                
                //datastream
                savong1
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator())
                .timeWindowAll(Time.seconds(range), Time.seconds(slide))
                //.trigger(PurgingTrigger.of(ContinuousEventTimeTrigger.of(Time.milliseconds(slide))))
                //.evictor(TimeEvictor.of(Time.milliseconds(slide)))
                .evictor(TimeEvictor.of(Time.seconds(slide)))
                //savong
                //.keyBy(0)
                //.countWindow(range,slide).trigger(PurgingTrigger.of(CountTrigger.of(slide)))//.evictor(CountEvictor.of(slide))
                .fold(new Integer (0), new FoldFunction<Tuple2<String, Integer>, Integer>() {
                   int round=0;
                   int chkp_count=1;
                   int chkp_round=0;
                   int rmd_tmp = rmd;
                   double g_value=0;
                   boolean full_window=false;
                   Node Tree = BST(checkpoint_size+rmd_tmp);
                   double[] cvalue = new double[n_checkpoints];
                   double[] pvalue = new double[num_slide];
                   Map<Integer, Double> pvalue_perslide1 = new HashMap<Integer, Double>();
                   Map<Integer, Double> pvalue_perslide2 = new HashMap<Integer, Double>();
                   int currTimestamp=0;
                   int currTimes_lastslide=-1;
                   @Override
                   public Integer fold(Integer acc, Tuple2<String, Integer> value) throws Exception{

                        
                        currTimestamp = Math.max(currTimestamp, Integer.parseInt(value.f0)/1000);//second
                        if(Integer.parseInt(value.f0)/1000 <= currTimes_lastslide + f1)
                        {
                            for(int k=0;k<num_slide;k++)
                            {
                                //if(inRange(slide*k, slide*(k+1), (Integer.parseInt(value.f0))%(range))) //millisecond
                                if(inRange(slide*k, slide*(k+1), (Integer.parseInt(value.f0)/1000)%(range+1))) //second
                                {
                                    //pvalue_perslide.put(k, (double)value.f1);
                                    if(pvalue_perslide1.containsKey(k))
                                        pvalue_perslide1.put(k, Math.max(pvalue_perslide1.get(k), value.f1));
                                    else
                                        pvalue_perslide1.put(k, (double)value.f1);
                                    break;
                                }
                            }
                        }
                        else if (Integer.parseInt(value.f0)/1000 <= currTimes_lastslide + f1 + f2)
                        {
                            for(int k=0;k<num_slide;k++)
                            {
                                //if(inRange(slide*k, slide*(k+1), (Integer.parseInt(value.f0))%(range))) //millisecond
                                if(inRange(slide*k, slide*(k+1), (Integer.parseInt(value.f0)/1000)%(range+1))) //second
                                {
                                    if(pvalue_perslide2.containsKey(k))
                                        pvalue_perslide2.put(k, Math.max(pvalue_perslide2.get(k), value.f1));
                                    else
                                        pvalue_perslide2.put(k, (double)value.f1);
                                    break;
                                }
                            }
                        }

                        

                        //if(round_tmp>=slide)
                        if(Integer.parseInt(value.f0)/1000 >= currTimes_lastslide + slide)
                        {
                            currTimes_lastslide = currTimestamp;
                            for (Map.Entry<Integer, Double> entry : pvalue_perslide1.entrySet())
                            {
                                //if late arrival records fall onto the respecitve pvalue and cvalue.
                                int low_bound = 0;
                                for(int j=0;j<n_checkpoints;j++)
                                {
                                    if(inRange(low_bound, checkpoint_size*(j+1)+rmd, entry.getKey()))
                                    {
                                        //cvalue[j]+=entry.getValue();
                                        cvalue[j] = Math.max(cvalue[j], entry.getValue());
                                        break;
                                    }
                                    low_bound = checkpoint_size*(j+1)+rmd;
                                }
                                //pvalue[entry.getKey()] += entry.getValue();
                                pvalue[entry.getKey()] = Math.max(pvalue[entry.getKey()], entry.getValue());
                                //g_value += entry.getValue();
                                g_value = Math.max(g_value, entry.getValue());
                                //if late arrival records fall onto the tree
                                //insert(Tree, entry.getKey()+1, entry.getValue(), 1);
                            }
                            pvalue_perslide1.clear();
                            

                            if(chkp_count >= (checkpoint_size+rmd_tmp))
                            {
                                chkp_count=0;
                                chkp_round++;
                                rmd_tmp=0;
                                if(chkp_round >= n_checkpoints)
                                {
                                    rmd_tmp = rmd;
                                    chkp_round=0;
                                }
                                if(full_window==true || round==num_slide-1)
                                {
                                   Tree = BST(checkpoint_size+rmd_tmp);
                                   int tmp_curptr=0;
                                   if(round==num_slide-1)
                                        tmp_curptr = 0;
                                   else
                                       tmp_curptr=round+1;
                                   for(int k=0;k<checkpoint_size+rmd_tmp;k++)
                                   {
                                       //pvalue[k+tmp_curptr] += pvalue[k+1+tmp_curptr];
                                       insert(Tree, k+1, pvalue[k+tmp_curptr], 1);
                                   }
                                   //insert(Tree, 2, 0, 1);
                                   g_value=0;
                                   cvalue[chkp_round]=0;
                                   for(int k=0;k<n_checkpoints;k++)
                                   {
                                       //g_value += cvalue[k];
                                       g_value = Math.max(g_value, cvalue[k]);
                                   }
                                }
                            }
                            chkp_count++;
                            round++;
                            if(round==num_slide)
                            {
                                full_window=true;
                                round=0;
                            }
                            
                            for (Map.Entry<Integer, Double> entry : pvalue_perslide2.entrySet())
                            {
                                //if late arrival records fall onto the respecitve pvalue and cvalue.
                                int low_bound = 0;
                                int part_tmp;
                                if(entry.getKey() == num_slide-1)
                                    part_tmp = 0;
                                else
                                    part_tmp = entry.getKey()+1;
                                for(int j=0;j<n_checkpoints;j++)
                                {
                                    if(inRange(low_bound, checkpoint_size*(j+1)+rmd, part_tmp))
                                    {
                                        //cvalue[j]+=entry.getValue();
                                        cvalue[j] = Math.max(cvalue[j], entry.getValue());
                                        break;
                                    }
                                    low_bound = checkpoint_size*(j+1)+rmd;
                                }
                                pvalue[part_tmp] = entry.getValue();
                                //g_value += entry.getValue();
                                g_value = Math.max(g_value, entry.getValue());
                            }
                            pvalue_perslide2.clear();
                            double results=0.0;
                            insert(Tree, chkp_count, 0, 1);
                            if(round < num_slide-1)
                                //results = g_value+Tree.aggregating;
                                results = Math.max(g_value, Tree.aggregating);
                            else
                                results = g_value;
                        acc = (int)results;
                        }
                        return acc;
                   }
                }).print();//.writeAsText("/Users/bousavong/Desktop/test/text/DEB12/bidirection.txt");//.print();
                }
        env.execute("Word Count Example");
        tEnd = System.currentTimeMillis();
        elapsedSeconds = (tEnd - tStart) / 1000.0;
        System.out.println("Time:"+elapsedSeconds);
        System.out.println("Finish");
    }
    public static void SUM(){
        // TODO code application logic here
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(1);
        
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        
      
        
        //DataStream<Tuple2<Integer,Integer>> datastream = env.readTextFile("/DEBS2012_Dataset/DEBS2012_129millions.txt")//DEB12/DEBS2012-ChallengeData.txt") //5000000.txt //50000.txt
        //.flatMap(new LineSplitter2());
        
        DataStream<Tuple2<String,Integer>> savong1 = env.socketTextStream("localhost", 9090)
                .flatMap(new NonFifoAggregation.LineSplitter());
        
        
        long tStart= System.currentTimeMillis();//=0;// = System.currentTimeMillis();
        long tEnd=0;
        Double elapsedSeconds=0.0;

        int range = 8; // window size
                int slide = 2;
                int f2 = range%slide;
                int f1 = slide - f2;
                if(f2==0)
                {
                int num_slide = range/slide;
                int n_checkpoints = (int)round(sqrt(num_slide));
                int checkpoint_size = num_slide/n_checkpoints;
                int rmd = num_slide%n_checkpoints;
                

                //datastream
                savong1
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator())
                .timeWindowAll(Time.seconds(range), Time.seconds(slide))
                //.trigger(PurgingTrigger.of(ContinuousEventTimeTrigger.of(Time.milliseconds(slide))))
                //.evictor(TimeEvictor.of(Time.milliseconds(slide)))      
                .evictor(TimeEvictor.of(Time.seconds(slide)))                
                //savong
                //.keyBy(0)
                //.countWindow(range,slide).trigger(PurgingTrigger.of(CountTrigger.of(slide)))//.evictor(CountEvictor.of(slide))
                .fold(new Integer (0), new FoldFunction<Tuple2<String, Integer>, Integer>() {
                   int round=0;
                   int chkp_count=0;
                   int chkp_round=0;
                   int rmd_tmp = rmd;
                   double g_value=0;
                   boolean full_window=false;
                   Node Tree = BST(checkpoint_size+rmd_tmp); 
                   double[] cvalue = new double[n_checkpoints]; 
                   double[] pvalue = new double[num_slide]; 
                   Map<Integer, Double> pvalue_perslide = new HashMap<Integer, Double>();
                   int currTimestamp=0;
                   int currTimes_lastslide=-1;
                   int currTimes_lastchpoint=0;
                   //double 
                   @Override
                   public Integer fold(Integer acc, Tuple2<String, Integer> value) throws Exception{
                        //if(count<=f1)
                        if(Integer.parseInt(value.f0)/1000 <= currTimes_lastslide + f1)
                        {
                            //currTimestamp = Math.max(currTimestamp, Integer.parseInt(value.f0));//millisecond
                            currTimestamp = Math.max(currTimestamp, Integer.parseInt(value.f0)/1000);//second
                            for(int k=0;k<num_slide;k++)
                            {
                                //if(inRange(slide*k, slide*(k+1), (Integer.parseInt(value.f0))%(range))) //millisecond
                                if(inRange(slide*k, slide*(k+1), (Integer.parseInt(value.f0)/1000)%(range))) //second
                                {
                                    //pvalue_perslide.put(k, (double)value.f1);
                                    if(pvalue_perslide.containsKey(k))
                                        pvalue_perslide.put(k, pvalue_perslide.get(k) + value.f1);
                                    else
                                        pvalue_perslide.put(k, (double)value.f1);
                                    break;
                                }
                            }
                        }

                        //if(round_tmp%slide==0)
                        //if(round_tmp>=slide)
                        if(Integer.parseInt(value.f0)/1000 >= currTimes_lastslide + slide)
                        {
                            chkp_count++;
                            for (Map.Entry<Integer, Double> entry : pvalue_perslide.entrySet())
                            {
                                //if late arrival records fall onto the respecitve pvalue and cvalue.
                                int low_bound = 0;
                                for(int j=0;j<n_checkpoints;j++)
                                {
                                    if(inRange(low_bound, checkpoint_size*(j+1)+rmd, entry.getKey()))
                                    {
                                        cvalue[j]+=entry.getValue();
                                        break;
                                    }
                                    low_bound = checkpoint_size*(j+1)+rmd;
                                }
                                pvalue[entry.getKey()] = entry.getValue();
                                g_value += entry.getValue();
                                //if late arrival records fall onto the tree
                                //insert(Tree, entry.getKey()+1, entry.getValue(), 1);  
                            }
                            pvalue_perslide.clear();
                        //    cvalue[chkp_round]+=f1_value;
                        //    pvalue[round] = f1_value;
                            insert(Tree, chkp_count, 0, 1);  
                            double results=0.0;
                            if(chkp_count < checkpoint_size+rmd_tmp)
                                results = g_value+Tree.aggregating;
                            else
                                results = g_value;

                            if(chkp_count >= checkpoint_size+rmd_tmp)
                            //if(Integer.parseInt(value.f0)/1000 >= currTimes_lastchpoint + checkpoint_size + rmd_tmp)
                            {
                                chkp_count=0;
                                chkp_round++;
                                rmd_tmp=0;
                                if(chkp_round >= n_checkpoints)
                                {
                                    rmd_tmp = rmd;
                                    chkp_round=0;
                                }
                                if(full_window==true || round==num_slide-1)
                                {
                                   Tree = BST(checkpoint_size+rmd_tmp); 
                                   //insert(Tree, round, 0, 1);  
                                   int tmp_curptr=0;
                                   if(round==num_slide-1)
                                       tmp_curptr = 0;
                                   else
                                       tmp_curptr=round+1;
                                   for(int k=0;k<checkpoint_size+rmd_tmp;k++)
                                   {
                                       //pvalue[k+tmp_curptr] += pvalue[k+1+tmp_curptr];
                                       insert(Tree, k+1, pvalue[k+tmp_curptr], 1);  
                                   }
                                   //insert(Tree, 2, 0, 1);  
                                   g_value=0;
                                   cvalue[chkp_round]=0;
                                   for(int k=0;k<n_checkpoints;k++)
                                   { 
                                       g_value += cvalue[k]; 
                                   }
                                }
                                currTimes_lastchpoint = currTimestamp;
                            }
                            round++;
                            if(round==num_slide)
                            {
                                full_window=true;
                                round=0; 
                            }
                               
                            //Integer result=0;//(int)Tree.aggregating;
                            acc = (int)results;
                            currTimes_lastslide = currTimestamp;
                        }
                        return acc;
                   }
                }).print();//.writeAsText("/Users/bousavong/Desktop/test/text/DEB12/bidirection.txt");//.print();
                }
                else
                {
                int num_slide = range/slide+1;
                int n_checkpoints = (int)round(sqrt(num_slide));
                int checkpoint_size = num_slide/n_checkpoints;
                int rmd = num_slide%n_checkpoints;
                
                
                //datastream
                savong1
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator())
                .timeWindowAll(Time.seconds(range), Time.seconds(slide))
                //.trigger(PurgingTrigger.of(ContinuousEventTimeTrigger.of(Time.milliseconds(slide))))
                //.evictor(TimeEvictor.of(Time.milliseconds(slide)))      
                .evictor(TimeEvictor.of(Time.seconds(slide)))   
                //savong
                //.keyBy(0)
                //.countWindow(range,slide).trigger(PurgingTrigger.of(CountTrigger.of(slide)))//.evictor(CountEvictor.of(slide))
                .fold(new Integer (0), new FoldFunction<Tuple2<String, Integer>, Integer>() {
                   int round=0;
                   int chkp_count=1;
                   int chkp_round=0;
                   int rmd_tmp = rmd;
                   double g_value=0;
                   boolean full_window=false;
                   Node Tree = BST(checkpoint_size+rmd_tmp); 
                   double[] cvalue = new double[n_checkpoints]; 
                   double[] pvalue = new double[num_slide];                 
                   Map<Integer, Double> pvalue_perslide1 = new HashMap<Integer, Double>();
                   Map<Integer, Double> pvalue_perslide2 = new HashMap<Integer, Double>();
                   int currTimestamp=0;
                   int currTimes_lastslide=-1;
                   @Override
                   public Integer fold(Integer acc, Tuple2<String, Integer> value) throws Exception{

                        
                        currTimestamp = Math.max(currTimestamp, Integer.parseInt(value.f0)/1000);//second
                        if(Integer.parseInt(value.f0)/1000 <= currTimes_lastslide + f1)
                        {
                            for(int k=0;k<num_slide;k++)
                            {
                                //if(inRange(slide*k, slide*(k+1), (Integer.parseInt(value.f0))%(range))) //millisecond
                                if(inRange(slide*k, slide*(k+1), (Integer.parseInt(value.f0)/1000)%(range+1))) //second
                                {
                                    //pvalue_perslide.put(k, (double)value.f1);
                                    if(pvalue_perslide1.containsKey(k))
                                        pvalue_perslide1.put(k, pvalue_perslide1.get(k) + value.f1);
                                    else
                                        pvalue_perslide1.put(k, (double)value.f1);
                                    break;
                                }
                            }
                        }
                        else if (Integer.parseInt(value.f0)/1000 <= currTimes_lastslide + f1 + f2)
                        {
                            for(int k=0;k<num_slide;k++)
                            {
                                //if(inRange(slide*k, slide*(k+1), (Integer.parseInt(value.f0))%(range))) //millisecond
                                if(inRange(slide*k, slide*(k+1), (Integer.parseInt(value.f0)/1000)%(range+1))) //second
                                {
                                    if(pvalue_perslide2.containsKey(k))
                                        pvalue_perslide2.put(k, pvalue_perslide2.get(k) + value.f1);
                                    else
                                        pvalue_perslide2.put(k, (double)value.f1);
                                    break;
                                }
                            }
                        }

                        

                        //if(round_tmp>=slide)
                        if(Integer.parseInt(value.f0)/1000 >= currTimes_lastslide + slide)
                        {
                            currTimes_lastslide = currTimestamp;
                            for (Map.Entry<Integer, Double> entry : pvalue_perslide1.entrySet())
                            {
                                //if late arrival records fall onto the respecitve pvalue and cvalue.
                                int low_bound = 0;
                                for(int j=0;j<n_checkpoints;j++)
                                {
                                    if(inRange(low_bound, checkpoint_size*(j+1)+rmd, entry.getKey()))
                                    {
                                        cvalue[j]+=entry.getValue();
                                        break;
                                    }
                                    low_bound = checkpoint_size*(j+1)+rmd;
                                }
                                pvalue[entry.getKey()] += entry.getValue();
                                g_value += entry.getValue();
                                //if late arrival records fall onto the tree
                                //insert(Tree, entry.getKey()+1, entry.getValue(), 1);  
                            }
                            pvalue_perslide1.clear();
                            

                            if(chkp_count >= (checkpoint_size+rmd_tmp))
                            {
                                chkp_count=0;
                                chkp_round++;
                                rmd_tmp=0;
                                if(chkp_round >= n_checkpoints)
                                {
                                    rmd_tmp = rmd;
                                    chkp_round=0;
                                }
                                if(full_window==true || round==num_slide-1)
                                {
                                   Tree = BST(checkpoint_size+rmd_tmp); 
                                   int tmp_curptr=0;
                                   if(round==num_slide-1)
                                        tmp_curptr = 0;
                                   else
                                       tmp_curptr=round+1;
                                   for(int k=0;k<checkpoint_size+rmd_tmp;k++)
                                   {
                                       //pvalue[k+tmp_curptr] += pvalue[k+1+tmp_curptr];
                                       insert(Tree, k+1, pvalue[k+tmp_curptr], 1);  
                                   }
                                   //insert(Tree, 2, 0, 1); 
                                   g_value=0;
                                   cvalue[chkp_round]=0;
                                   for(int k=0;k<n_checkpoints;k++)
                                   { 
                                       g_value += cvalue[k]; 
                                   }
                                }
                            }
                            chkp_count++;
                            round++;
                            if(round==num_slide)
                            {
                                full_window=true;
                                round=0; 
                            }
                            
                            for (Map.Entry<Integer, Double> entry : pvalue_perslide2.entrySet())
                            {
                                //if late arrival records fall onto the respecitve pvalue and cvalue.
                                int low_bound = 0;
                                int part_tmp;
                                if(entry.getKey() == num_slide-1)
                                    part_tmp = 0;
                                else
                                    part_tmp = entry.getKey()+1;
                                for(int j=0;j<n_checkpoints;j++)
                                {
                                    if(inRange(low_bound, checkpoint_size*(j+1)+rmd, part_tmp))
                                    {
                                        cvalue[j]+=entry.getValue();
                                        break;
                                    }
                                    low_bound = checkpoint_size*(j+1)+rmd;
                                }
                                pvalue[part_tmp] = entry.getValue();
                                g_value += entry.getValue();
                            }
                            pvalue_perslide2.clear();
                            double results=0.0;
                            insert(Tree, chkp_count, 0, 1);  
                            if(round < num_slide-1)
                                results = g_value+Tree.aggregating;
                            else
                                results = g_value;
                        acc = (int)results;
                        }
                        return acc;
                   }
                }).print();//.writeAsText("/Users/bousavong/Desktop/test/text/DEB12/bidirection.txt");//.print();
                }
        env.execute("Word Count Example");
        tEnd = System.currentTimeMillis();
        elapsedSeconds = (tEnd - tStart) / 1000.0;
        System.out.println("Time:"+elapsedSeconds);
        System.out.println("Finish");
    }
//    public static class LineSplitter implements FlatMapFunction<String, Tuple11<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> {
//        @Override
//        public void flatMap(String line, Collector<Tuple11<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> out) {
//            String[] cells = line.split("\\s+");
//            out.collect(new Tuple11<>(1, Integer.parseInt(cells[1]), Integer.parseInt(cells[2]), Integer.parseInt(cells[3]), Integer.parseInt(cells[4]), Integer.parseInt(cells[5]), Integer.parseInt(cells[6]), Integer.parseInt(cells[7]), Integer.parseInt(cells[8]), Integer.parseInt(cells[9]), Integer.parseInt(cells[10])));
//        }
//    }

//    public static class LineSplitter2 implements FlatMapFunction<String, Tuple2<Integer, Integer>> {
//        @Override
//        public void flatMap(String line, Collector<Tuple2<Integer, Integer>> out) {
//            String[] cells = line.split("\\s+");
//            //out.collect(new Tuple2<>(Integer.parseInt(cells[1]), Integer.parseInt(cells[2])));
//            out.collect(new Tuple2<>(1, Integer.parseInt(cells[2])));
//        }
//    }
    //f1!=0, f20=0
    public static void insert(Node node, double tosearch, double aggregate, int f1f2) {
        //temperarily test
        //temperarily test
        if (tosearch < node.value) {
            if (node.left != null) {
                insert(node.left, tosearch, aggregate, f1f2);
                node.aggregating = node.left.aggregating+node.right.aggregating;
            }
        } 
        else if (tosearch > node.value) {
            if (node.right != null) {
                insert(node.right, tosearch, aggregate, f1f2);
                node.aggregating = node.left.aggregating+node.right.aggregating;
            }
        }
        else
        {
            node.aggregating = aggregate;
        }  
    }
    //f1!=, f2==0;

    //f1!=0, f2!=0
    public static void insert2(Node node, double tosearch, double aggregate, int f1f2) {
        if (tosearch < node.value) {
        if (node.left != null) {
            insert2(node.left, tosearch, aggregate, f1f2);
            node.aggregating = node.left.aggregating+node.right.aggregating;
            }
        } else if (tosearch > node.value) {
            if (node.right != null) {
                insert2(node.right, tosearch, aggregate, f1f2);
                node.aggregating = node.left.aggregating+node.right.aggregating;
            }
        }
        else
        {
            if(f1f2==1) //if f1_value, sum
                node.aggregating += aggregate;
            else //if f2_value, overwrite
                node.aggregating = aggregate;
        }

    }
    //f1!=, f2!=0;
    public static class Node implements Serializable
    {
        Node left;
        Node right;
        double value;
        double aggregating;
        double biggest_leaf_node;
        double smallest_leaf_node;
        public Node(double value) {
            this.value = value;
        }
    }
    static Node BST(Integer num_slide)
    {
        Node Tree;
        HashMap<Integer, Node> Leaf_nodes = new HashMap<>();
        Node tree = new Node(0);
        for(int i=1;i<=num_slide;i++)
        {
            Node level_node = new Node(i);
            level_node.biggest_leaf_node = i;
            level_node.smallest_leaf_node = i;
            Leaf_nodes.put(i, level_node);
        }
        while(true)
        {
            HashMap<Integer, Node> Leaf_nodes_tmp = new HashMap<>();
            for(int i=1;i<=Leaf_nodes.size();i++)
            {
                if(i%2==1)//(k==1)
                {
                    if(i<Leaf_nodes.size())
                    {
                        Node tmp1 = new Node(0);
                        tmp1.left= Leaf_nodes.get(i);
                        tmp1.value+=Leaf_nodes.get(i).value;
                        tmp1.biggest_leaf_node = Leaf_nodes.get(i).biggest_leaf_node;
                        tmp1.smallest_leaf_node = Leaf_nodes.get(i).smallest_leaf_node;
                        Leaf_nodes_tmp.put(Leaf_nodes_tmp.size()+1, tmp1);
                    }
                    else
                    {
                        Leaf_nodes_tmp.put(Leaf_nodes_tmp.size()+1, Leaf_nodes.get(i));
                    }
                }
                else// if(k==2)
                {
                    Leaf_nodes_tmp.get(Leaf_nodes_tmp.size()).right=Leaf_nodes.get(i);//tmp1;
                    //Leaf_nodes_tmp.get(Leaf_nodes_tmp.size()).value =(Leaf_nodes_tmp.get(Leaf_nodes_tmp.size()).value+Leaf_nodes.get(i).value)/2;
                    Leaf_nodes_tmp.get(Leaf_nodes_tmp.size()).value =(Leaf_nodes_tmp.get(Leaf_nodes_tmp.size()).biggest_leaf_node+Leaf_nodes.get(i).smallest_leaf_node)/2;
                    Leaf_nodes_tmp.get(Leaf_nodes_tmp.size()).biggest_leaf_node = Leaf_nodes.get(i).biggest_leaf_node;
                    Leaf_nodes_tmp.get(Leaf_nodes_tmp.size()).smallest_leaf_node = Leaf_nodes_tmp.get(Leaf_nodes_tmp.size()).smallest_leaf_node;
                }   
            }
            Leaf_nodes = Leaf_nodes_tmp;
            if(Leaf_nodes.size()==1)
            {
                Tree=Leaf_nodes.get(1);
                break;
            } 
        }
        return Tree;
    }
    public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
            if(line!=null || line!="")
            {
                String[] word = line.split(",",0);
                out.collect(new Tuple2<String, Integer>(word[0], Integer.parseInt(word[1])));
            }
        }
    }
    
public static class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<Tuple2<String, Integer>> {

    private final long maxOutOfOrderness = 1; // 3.5 seconds

    private long currentMaxTimestamp;
    public long extractAscendingTimestamp(Tuple2<String, Integer> element) {
        return Long.parseLong(element.f0);
    }

    @Override
    public long extractTimestamp(Tuple2<String, Integer> element, long previousElementTimestamp) {
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

public static class TimeLagWatermarkGenerator implements AssignerWithPeriodicWatermarks<Tuple2<String, Integer>> {

	private final long maxTimeLag = 0; // 5 seconds

	@Override
	public long extractTimestamp(Tuple2<String, Integer> element, long previousElementTimestamp) {
		return Long.parseLong(element.f0);
	}

	@Override
	public Watermark getCurrentWatermark() {
		// return the watermark as current time minus the maximum time lag
		return new Watermark(System.currentTimeMillis() - maxTimeLag);
	}
}
public static boolean inRange(double low, double high, double x) 
{ 
    boolean ingap=false;
    if(low<=x && x< high)
        ingap = true;
    else
        ingap = false;
    return  ingap;//((x-low) < (high-low)); 
    
}
    
    

    
    public static void MIN(){
        // TODO code application logic here
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(1);
        
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        
      
        
        //DataStream<Tuple2<Integer,Integer>> datastream = env.readTextFile("/DEBS2012_Dataset/DEBS2012_129millions.txt")//DEB12/DEBS2012-ChallengeData.txt") //5000000.txt //50000.txt
        //.flatMap(new LineSplitter2());
        
        DataStream<Tuple2<String,Integer>> savong1 = env.socketTextStream("localhost", 9090)
                .flatMap(new NonFifoAggregation.LineSplitter());
        
        
        long tStart= System.currentTimeMillis();//=0;// = System.currentTimeMillis();
        long tEnd=0;
        Double elapsedSeconds=0.0;

        int range = 8; // window size
                int slide = 2;
                int f2 = range%slide;
                int f1 = slide - f2;
                if(f2==0)
                {
                int num_slide = range/slide;
                int n_checkpoints = (int)round(sqrt(num_slide));
                int checkpoint_size = num_slide/n_checkpoints;
                int rmd = num_slide%n_checkpoints;
                

                //datastream
                savong1
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator())
                .timeWindowAll(Time.seconds(range), Time.seconds(slide))
                //.trigger(PurgingTrigger.of(ContinuousEventTimeTrigger.of(Time.milliseconds(slide))))
                //.evictor(TimeEvictor.of(Time.milliseconds(slide)))
                .evictor(TimeEvictor.of(Time.seconds(slide)))
                //savong
                //.keyBy(0)
                //.countWindow(range,slide).trigger(PurgingTrigger.of(CountTrigger.of(slide)))//.evictor(CountEvictor.of(slide))
                .fold(new Integer (0), new FoldFunction<Tuple2<String, Integer>, Integer>() {
                   int round=0;
                   int chkp_count=0;
                   int chkp_round=0;
                   int rmd_tmp = rmd;
                   double g_value=0;
                   boolean full_window=false;
                   Node Tree = BST(checkpoint_size+rmd_tmp);
                   double[] cvalue = new double[n_checkpoints];
                   double[] pvalue = new double[num_slide];
                   Map<Integer, Double> pvalue_perslide = new HashMap<Integer, Double>();
                   int currTimestamp=0;
                   int currTimes_lastslide=-1;
                   int currTimes_lastchpoint=0;
                   //double
                   @Override
                   public Integer fold(Integer acc, Tuple2<String, Integer> value) throws Exception{
                        //if(count<=f1)
                        if(Integer.parseInt(value.f0)/1000 <= currTimes_lastslide + f1)
                        {
                            //currTimestamp = Math.max(currTimestamp, Integer.parseInt(value.f0));//millisecond
                            currTimestamp = Math.max(currTimestamp, Integer.parseInt(value.f0)/1000);//second
                            for(int k=0;k<num_slide;k++)
                            {
                                //if(inRange(slide*k, slide*(k+1), (Integer.parseInt(value.f0))%(range))) //millisecond
                                if(inRange(slide*k, slide*(k+1), (Integer.parseInt(value.f0)/1000)%(range))) //second
                                {
                                    //pvalue_perslide.put(k, (double)value.f1);
                                    if(pvalue_perslide.containsKey(k))
                                        pvalue_perslide.put(k, Math.min(pvalue_perslide.get(k), value.f1));
                                    else
                                        pvalue_perslide.put(k, (double)value.f1);
                                    break;
                                }
                            }
                        }

                        //if(round_tmp%slide==0)
                        //if(round_tmp>=slide)
                        if(Integer.parseInt(value.f0)/1000 >= currTimes_lastslide + slide)
                        {
                            chkp_count++;
                            for (Map.Entry<Integer, Double> entry : pvalue_perslide.entrySet())
                            {
                                //if late arrival records fall onto the respecitve pvalue and cvalue.
                                int low_bound = 0;
                                for(int j=0;j<n_checkpoints;j++)
                                {
                                    if(inRange(low_bound, checkpoint_size*(j+1)+rmd, entry.getKey()))
                                    {
                                        //cvalue[j]+=entry.getValue();
                                        cvalue[j] = Math.min(cvalue[j], entry.getValue());
                                        break;
                                    }
                                    low_bound = checkpoint_size*(j+1)+rmd;
                                }
                                pvalue[entry.getKey()] = entry.getValue();
                                //g_value += entry.getValue();
                                g_value = Math.min(g_value, entry.getValue());
                                //if late arrival records fall onto the tree
                                //insert(Tree, entry.getKey()+1, entry.getValue(), 1);
                            }
                            pvalue_perslide.clear();
                        //    cvalue[chkp_round]+=f1_value;
                        //    pvalue[round] = f1_value;
                            insert(Tree, chkp_count, 0, 1);
                            double results=0.0;
                            if(chkp_count < checkpoint_size+rmd_tmp)
                                //results = g_value+Tree.aggregating;
                                results = Math.min(g_value, Tree.aggregating);
                            else
                                results = g_value;

                            if(chkp_count >= checkpoint_size+rmd_tmp)
                            //if(Integer.parseInt(value.f0)/1000 >= currTimes_lastchpoint + checkpoint_size + rmd_tmp)
                            {
                                chkp_count=0;
                                chkp_round++;
                                rmd_tmp=0;
                                if(chkp_round >= n_checkpoints)
                                {
                                    rmd_tmp = rmd;
                                    chkp_round=0;
                                }
                                if(full_window==true || round==num_slide-1)
                                {
                                   Tree = BST(checkpoint_size+rmd_tmp);
                                   //insert(Tree, round, 0, 1);
                                   int tmp_curptr=0;
                                   if(round==num_slide-1)
                                       tmp_curptr = 0;
                                   else
                                       tmp_curptr=round+1;
                                   for(int k=0;k<checkpoint_size+rmd_tmp;k++)
                                   {
                                       //pvalue[k+tmp_curptr] += pvalue[k+1+tmp_curptr];
                                       insert(Tree, k+1, pvalue[k+tmp_curptr], 1);
                                   }
                                   //insert(Tree, 2, 0, 1);
                                   g_value=0;
                                   cvalue[chkp_round]=0;
                                   for(int k=0;k<n_checkpoints;k++)
                                   {
                                       //g_value += cvalue[k];
                                       g_value = Math.min(g_value, cvalue[k]);
                                   }
                                }
                                currTimes_lastchpoint = currTimestamp;
                            }
                            round++;
                            if(round==num_slide)
                            {
                                full_window=true;
                                round=0;
                            }
                               
                            //Integer result=0;//(int)Tree.aggregating;
                            acc = (int)results;
                            currTimes_lastslide = currTimestamp;
                        }
                        return acc;
                   }
                }).print();//.writeAsText("/Users/bousavong/Desktop/test/text/DEB12/bidirection.txt");//.print();
                }
                else
                {
                int num_slide = range/slide+1;
                int n_checkpoints = (int)round(sqrt(num_slide));
                int checkpoint_size = num_slide/n_checkpoints;
                int rmd = num_slide%n_checkpoints;
                
                
                //datastream
                savong1
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator())
                .timeWindowAll(Time.seconds(range), Time.seconds(slide))
                //.trigger(PurgingTrigger.of(ContinuousEventTimeTrigger.of(Time.milliseconds(slide))))
                //.evictor(TimeEvictor.of(Time.milliseconds(slide)))
                .evictor(TimeEvictor.of(Time.seconds(slide)))
                //savong
                //.keyBy(0)
                //.countWindow(range,slide).trigger(PurgingTrigger.of(CountTrigger.of(slide)))//.evictor(CountEvictor.of(slide))
                .fold(new Integer (0), new FoldFunction<Tuple2<String, Integer>, Integer>() {
                   int round=0;
                   int chkp_count=1;
                   int chkp_round=0;
                   int rmd_tmp = rmd;
                   double g_value=0;
                   boolean full_window=false;
                   Node Tree = BST(checkpoint_size+rmd_tmp);
                   double[] cvalue = new double[n_checkpoints];
                   double[] pvalue = new double[num_slide];
                   Map<Integer, Double> pvalue_perslide1 = new HashMap<Integer, Double>();
                   Map<Integer, Double> pvalue_perslide2 = new HashMap<Integer, Double>();
                   int currTimestamp=0;
                   int currTimes_lastslide=-1;
                   @Override
                   public Integer fold(Integer acc, Tuple2<String, Integer> value) throws Exception{

                        
                        currTimestamp = Math.min(currTimestamp, Integer.parseInt(value.f0)/1000);//second
                        if(Integer.parseInt(value.f0)/1000 <= currTimes_lastslide + f1)
                        {
                            for(int k=0;k<num_slide;k++)
                            {
                                //if(inRange(slide*k, slide*(k+1), (Integer.parseInt(value.f0))%(range))) //millisecond
                                if(inRange(slide*k, slide*(k+1), (Integer.parseInt(value.f0)/1000)%(range+1))) //second
                                {
                                    //pvalue_perslide.put(k, (double)value.f1);
                                    if(pvalue_perslide1.containsKey(k))
                                        pvalue_perslide1.put(k, Math.min(pvalue_perslide1.get(k), value.f1));
                                    else
                                        pvalue_perslide1.put(k, (double)value.f1);
                                    break;
                                }
                            }
                        }
                        else if (Integer.parseInt(value.f0)/1000 <= currTimes_lastslide + f1 + f2)
                        {
                            for(int k=0;k<num_slide;k++)
                            {
                                //if(inRange(slide*k, slide*(k+1), (Integer.parseInt(value.f0))%(range))) //millisecond
                                if(inRange(slide*k, slide*(k+1), (Integer.parseInt(value.f0)/1000)%(range+1))) //second
                                {
                                    if(pvalue_perslide2.containsKey(k))
                                        pvalue_perslide2.put(k, Math.min(pvalue_perslide2.get(k), value.f1));
                                    else
                                        pvalue_perslide2.put(k, (double)value.f1);
                                    break;
                                }
                            }
                        }

                        

                        //if(round_tmp>=slide)
                        if(Integer.parseInt(value.f0)/1000 >= currTimes_lastslide + slide)
                        {
                            currTimes_lastslide = currTimestamp;
                            for (Map.Entry<Integer, Double> entry : pvalue_perslide1.entrySet())
                            {
                                //if late arrival records fall onto the respecitve pvalue and cvalue.
                                int low_bound = 0;
                                for(int j=0;j<n_checkpoints;j++)
                                {
                                    if(inRange(low_bound, checkpoint_size*(j+1)+rmd, entry.getKey()))
                                    {
                                        //cvalue[j]+=entry.getValue();
                                        cvalue[j] = Math.min(cvalue[j], entry.getValue());
                                        break;
                                    }
                                    low_bound = checkpoint_size*(j+1)+rmd;
                                }
                                //pvalue[entry.getKey()] += entry.getValue();
                                pvalue[entry.getKey()] = Math.min(pvalue[entry.getKey()], entry.getValue());
                                //g_value += entry.getValue();
                                g_value = Math.min(g_value, entry.getValue());
                                //if late arrival records fall onto the tree
                                //insert(Tree, entry.getKey()+1, entry.getValue(), 1);
                            }
                            pvalue_perslide1.clear();
                            

                            if(chkp_count >= (checkpoint_size+rmd_tmp))
                            {
                                chkp_count=0;
                                chkp_round++;
                                rmd_tmp=0;
                                if(chkp_round >= n_checkpoints)
                                {
                                    rmd_tmp = rmd;
                                    chkp_round=0;
                                }
                                if(full_window==true || round==num_slide-1)
                                {
                                   Tree = BST(checkpoint_size+rmd_tmp);
                                   int tmp_curptr=0;
                                   if(round==num_slide-1)
                                        tmp_curptr = 0;
                                   else
                                       tmp_curptr=round+1;
                                   for(int k=0;k<checkpoint_size+rmd_tmp;k++)
                                   {
                                       //pvalue[k+tmp_curptr] += pvalue[k+1+tmp_curptr];
                                       insert(Tree, k+1, pvalue[k+tmp_curptr], 1);
                                   }
                                   //insert(Tree, 2, 0, 1);
                                   g_value=0;
                                   cvalue[chkp_round]=0;
                                   for(int k=0;k<n_checkpoints;k++)
                                   {
                                       //g_value += cvalue[k];
                                       g_value = Math.min(g_value, cvalue[k]);
                                   }
                                }
                            }
                            chkp_count++;
                            round++;
                            if(round==num_slide)
                            {
                                full_window=true;
                                round=0;
                            }
                            
                            for (Map.Entry<Integer, Double> entry : pvalue_perslide2.entrySet())
                            {
                                //if late arrival records fall onto the respecitve pvalue and cvalue.
                                int low_bound = 0;
                                int part_tmp;
                                if(entry.getKey() == num_slide-1)
                                    part_tmp = 0;
                                else
                                    part_tmp = entry.getKey()+1;
                                for(int j=0;j<n_checkpoints;j++)
                                {
                                    if(inRange(low_bound, checkpoint_size*(j+1)+rmd, part_tmp))
                                    {
                                        //cvalue[j]+=entry.getValue();
                                        cvalue[j] = Math.min(cvalue[j], entry.getValue());
                                        break;
                                    }
                                    low_bound = checkpoint_size*(j+1)+rmd;
                                }
                                pvalue[part_tmp] = entry.getValue();
                                //g_value += entry.getValue();
                                g_value = Math.min(g_value, entry.getValue());
                            }
                            pvalue_perslide2.clear();
                            double results=0.0;
                            insert(Tree, chkp_count, 0, 1);
                            if(round < num_slide-1)
                                //results = g_value+Tree.aggregating;
                                results = Math.min(g_value, Tree.aggregating);
                            else
                                results = g_value;
                        acc = (int)results;
                        }
                        return acc;
                   }
                }).print();//.writeAsText("/Users/bousavong/Desktop/test/text/DEB12/bidirection.txt");//.print();
                }
        env.execute("Word Count Example");
        tEnd = System.currentTimeMillis();
        elapsedSeconds = (tEnd - tStart) / 1000.0;
        System.out.println("Time:"+elapsedSeconds);
        System.out.println("Finish");
    }
    
    public static void AVERAGE(){
        // TODO code application logic here
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(1);
        
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        
      
        
        //DataStream<Tuple2<Integer,Integer>> datastream = env.readTextFile("/DEBS2012_Dataset/DEBS2012_129millions.txt")//DEB12/DEBS2012-ChallengeData.txt") //5000000.txt //50000.txt
        //.flatMap(new LineSplitter2());
        
        DataStream<Tuple2<String,Integer>> savong1 = env.socketTextStream("localhost", 9090)
                .flatMap(new NonFifoAggregation.LineSplitter());
        
        
        long tStart= System.currentTimeMillis();//=0;// = System.currentTimeMillis();
        long tEnd=0;
        Double elapsedSeconds=0.0;

        int range = 8; // window size
                int slide = 2;
                int f2 = range%slide;
                int f1 = slide - f2;
                if(f2==0)
                {
                int num_slide = range/slide;
                int n_checkpoints = (int)round(sqrt(num_slide));
                int checkpoint_size = num_slide/n_checkpoints;
                int rmd = num_slide%n_checkpoints;
                

                //datastream
                savong1
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator())
                .timeWindowAll(Time.seconds(range), Time.seconds(slide))
                //.trigger(PurgingTrigger.of(ContinuousEventTimeTrigger.of(Time.milliseconds(slide))))
                //.evictor(TimeEvictor.of(Time.milliseconds(slide)))
                .evictor(TimeEvictor.of(Time.seconds(slide)))
                //savong
                //.keyBy(0)
                //.countWindow(range,slide).trigger(PurgingTrigger.of(CountTrigger.of(slide)))//.evictor(CountEvictor.of(slide))
                .fold(new Integer (0), new FoldFunction<Tuple2<String, Integer>, Integer>() {
                   int count=0;
                int round=0;
                   int chkp_count=0;
                   int chkp_round=0;
                   int rmd_tmp = rmd;
                   double g_value=0;
                   boolean full_window=false;
                   Node Tree = BST(checkpoint_size+rmd_tmp);
                   double[] cvalue = new double[n_checkpoints];
                   double[] pvalue = new double[num_slide];
                   Map<Integer, Double> pvalue_perslide = new HashMap<Integer, Double>();
                   int currTimestamp=0;
                   int currTimes_lastslide=-1;
                   int currTimes_lastchpoint=0;
                   //double
                   @Override
                   public Integer fold(Integer acc, Tuple2<String, Integer> value) throws Exception{
                       count++;
                       //if(count<=f1)
                        if(Integer.parseInt(value.f0)/1000 <= currTimes_lastslide + f1)
                        {
                            //currTimestamp = Math.max(currTimestamp, Integer.parseInt(value.f0));//millisecond
                            currTimestamp = Math.max(currTimestamp, Integer.parseInt(value.f0)/1000);//second
                            for(int k=0;k<num_slide;k++)
                            {
                                //if(inRange(slide*k, slide*(k+1), (Integer.parseInt(value.f0))%(range))) //millisecond
                                if(inRange(slide*k, slide*(k+1), (Integer.parseInt(value.f0)/1000)%(range))) //second
                                {
                                    //pvalue_perslide.put(k, (double)value.f1);
                                    if(pvalue_perslide.containsKey(k))
                                        pvalue_perslide.put(k, pvalue_perslide.get(k) + value.f1);
                                    else
                                        pvalue_perslide.put(k, (double)value.f1);
                                    break;
                                }
                            }
                        }

                        //if(round_tmp%slide==0)
                        //if(round_tmp>=slide)
                        if(Integer.parseInt(value.f0)/1000 >= currTimes_lastslide + slide)
                        {
                            chkp_count++;
                            for (Map.Entry<Integer, Double> entry : pvalue_perslide.entrySet())
                            {
                                //if late arrival records fall onto the respecitve pvalue and cvalue.
                                int low_bound = 0;
                                for(int j=0;j<n_checkpoints;j++)
                                {
                                    if(inRange(low_bound, checkpoint_size*(j+1)+rmd, entry.getKey()))
                                    {
                                        cvalue[j]+=entry.getValue();
                                        break;
                                    }
                                    low_bound = checkpoint_size*(j+1)+rmd;
                                }
                                pvalue[entry.getKey()] = entry.getValue();
                                g_value += entry.getValue();
                                //if late arrival records fall onto the tree
                                //insert(Tree, entry.getKey()+1, entry.getValue(), 1);
                            }
                            pvalue_perslide.clear();
                        //    cvalue[chkp_round]+=f1_value;
                        //    pvalue[round] = f1_value;
                            insert(Tree, chkp_count, 0, 1);
                            double results=0.0;
                            if(chkp_count < checkpoint_size+rmd_tmp)
                                results = g_value+Tree.aggregating;
                            else
                                results = g_value;

                            if(chkp_count >= checkpoint_size+rmd_tmp)
                            //if(Integer.parseInt(value.f0)/1000 >= currTimes_lastchpoint + checkpoint_size + rmd_tmp)
                            {
                                chkp_count=0;
                                chkp_round++;
                                rmd_tmp=0;
                                if(chkp_round >= n_checkpoints)
                                {
                                    rmd_tmp = rmd;
                                    chkp_round=0;
                                }
                                if(full_window==true || round==num_slide-1)
                                {
                                   Tree = BST(checkpoint_size+rmd_tmp);
                                   //insert(Tree, round, 0, 1);
                                   int tmp_curptr=0;
                                   if(round==num_slide-1)
                                       tmp_curptr = 0;
                                   else
                                       tmp_curptr=round+1;
                                   for(int k=0;k<checkpoint_size+rmd_tmp;k++)
                                   {
                                       //pvalue[k+tmp_curptr] += pvalue[k+1+tmp_curptr];
                                       insert(Tree, k+1, pvalue[k+tmp_curptr], 1);
                                   }
                                   //insert(Tree, 2, 0, 1);
                                   g_value=0;
                                   cvalue[chkp_round]=0;
                                   for(int k=0;k<n_checkpoints;k++)
                                   {
                                       g_value += cvalue[k];
                                   }
                                }
                                currTimes_lastchpoint = currTimestamp;
                            }
                            round++;
                            if(round==num_slide)
                            {
                                full_window=true;
                                round=0;
                            }
                               
                            //Integer result=0;//(int)Tree.aggregating;
                            acc = (int)results/count;
                            currTimes_lastslide = currTimestamp;
                            count=0;
                        }
                        return acc;
                   }
                }).print();//.writeAsText("/Users/bousavong/Desktop/test/text/DEB12/bidirection.txt");//.print();
                }
                else
                {
                int num_slide = range/slide+1;
                int n_checkpoints = (int)round(sqrt(num_slide));
                int checkpoint_size = num_slide/n_checkpoints;
                int rmd = num_slide%n_checkpoints;
                
                
                //datastream
                savong1
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator())
                .timeWindowAll(Time.seconds(range), Time.seconds(slide))
                //.trigger(PurgingTrigger.of(ContinuousEventTimeTrigger.of(Time.milliseconds(slide))))
                //.evictor(TimeEvictor.of(Time.milliseconds(slide)))
                .evictor(TimeEvictor.of(Time.seconds(slide)))
                //savong
                //.keyBy(0)
                //.countWindow(range,slide).trigger(PurgingTrigger.of(CountTrigger.of(slide)))//.evictor(CountEvictor.of(slide))
                .fold(new Integer (0), new FoldFunction<Tuple2<String, Integer>, Integer>() {
                    int count=0;
                    int round=0;
                   int chkp_count=1;
                   int chkp_round=0;
                   int rmd_tmp = rmd;
                   double g_value=0;
                   boolean full_window=false;
                   Node Tree = BST(checkpoint_size+rmd_tmp);
                   double[] cvalue = new double[n_checkpoints];
                   double[] pvalue = new double[num_slide];
                   Map<Integer, Double> pvalue_perslide1 = new HashMap<Integer, Double>();
                   Map<Integer, Double> pvalue_perslide2 = new HashMap<Integer, Double>();
                   int currTimestamp=0;
                   int currTimes_lastslide=-1;
                   @Override
                   public Integer fold(Integer acc, Tuple2<String, Integer> value) throws Exception{

                       count++;
                        currTimestamp = Math.max(currTimestamp, Integer.parseInt(value.f0)/1000);//second
                        if(Integer.parseInt(value.f0)/1000 <= currTimes_lastslide + f1)
                        {
                            for(int k=0;k<num_slide;k++)
                            {
                                //if(inRange(slide*k, slide*(k+1), (Integer.parseInt(value.f0))%(range))) //millisecond
                                if(inRange(slide*k, slide*(k+1), (Integer.parseInt(value.f0)/1000)%(range+1))) //second
                                {
                                    //pvalue_perslide.put(k, (double)value.f1);
                                    if(pvalue_perslide1.containsKey(k))
                                        pvalue_perslide1.put(k, pvalue_perslide1.get(k) + value.f1);
                                    else
                                        pvalue_perslide1.put(k, (double)value.f1);
                                    break;
                                }
                            }
                        }
                        else if (Integer.parseInt(value.f0)/1000 <= currTimes_lastslide + f1 + f2)
                        {
                            for(int k=0;k<num_slide;k++)
                            {
                                //if(inRange(slide*k, slide*(k+1), (Integer.parseInt(value.f0))%(range))) //millisecond
                                if(inRange(slide*k, slide*(k+1), (Integer.parseInt(value.f0)/1000)%(range+1))) //second
                                {
                                    if(pvalue_perslide2.containsKey(k))
                                        pvalue_perslide2.put(k, pvalue_perslide2.get(k) + value.f1);
                                    else
                                        pvalue_perslide2.put(k, (double)value.f1);
                                    break;
                                }
                            }
                        }

                        

                        //if(round_tmp>=slide)
                        if(Integer.parseInt(value.f0)/1000 >= currTimes_lastslide + slide)
                        {
                            currTimes_lastslide = currTimestamp;
                            for (Map.Entry<Integer, Double> entry : pvalue_perslide1.entrySet())
                            {
                                //if late arrival records fall onto the respecitve pvalue and cvalue.
                                int low_bound = 0;
                                for(int j=0;j<n_checkpoints;j++)
                                {
                                    if(inRange(low_bound, checkpoint_size*(j+1)+rmd, entry.getKey()))
                                    {
                                        cvalue[j]+=entry.getValue();
                                        break;
                                    }
                                    low_bound = checkpoint_size*(j+1)+rmd;
                                }
                                pvalue[entry.getKey()] += entry.getValue();
                                g_value += entry.getValue();
                                //if late arrival records fall onto the tree
                                //insert(Tree, entry.getKey()+1, entry.getValue(), 1);
                            }
                            pvalue_perslide1.clear();
                            

                            if(chkp_count >= (checkpoint_size+rmd_tmp))
                            {
                                chkp_count=0;
                                chkp_round++;
                                rmd_tmp=0;
                                if(chkp_round >= n_checkpoints)
                                {
                                    rmd_tmp = rmd;
                                    chkp_round=0;
                                }
                                if(full_window==true || round==num_slide-1)
                                {
                                   Tree = BST(checkpoint_size+rmd_tmp);
                                   int tmp_curptr=0;
                                   if(round==num_slide-1)
                                        tmp_curptr = 0;
                                   else
                                       tmp_curptr=round+1;
                                   for(int k=0;k<checkpoint_size+rmd_tmp;k++)
                                   {
                                       //pvalue[k+tmp_curptr] += pvalue[k+1+tmp_curptr];
                                       insert(Tree, k+1, pvalue[k+tmp_curptr], 1);
                                   }
                                   //insert(Tree, 2, 0, 1);
                                   g_value=0;
                                   cvalue[chkp_round]=0;
                                   for(int k=0;k<n_checkpoints;k++)
                                   {
                                       g_value += cvalue[k];
                                   }
                                }
                            }
                            chkp_count++;
                            round++;
                            if(round==num_slide)
                            {
                                full_window=true;
                                round=0;
                            }
                            
                            for (Map.Entry<Integer, Double> entry : pvalue_perslide2.entrySet())
                            {
                                //if late arrival records fall onto the respecitve pvalue and cvalue.
                                int low_bound = 0;
                                int part_tmp;
                                if(entry.getKey() == num_slide-1)
                                    part_tmp = 0;
                                else
                                    part_tmp = entry.getKey()+1;
                                for(int j=0;j<n_checkpoints;j++)
                                {
                                    if(inRange(low_bound, checkpoint_size*(j+1)+rmd, part_tmp))
                                    {
                                        cvalue[j]+=entry.getValue();
                                        break;
                                    }
                                    low_bound = checkpoint_size*(j+1)+rmd;
                                }
                                pvalue[part_tmp] = entry.getValue();
                                g_value += entry.getValue();
                            }
                            pvalue_perslide2.clear();
                            double results=0.0;
                            insert(Tree, chkp_count, 0, 1);
                            if(round < num_slide-1)
                                results = g_value+Tree.aggregating;
                            else
                                results = g_value;
                        acc = (int)results/count;
                        count=0;
                        }
                        return acc;
                   }
                }).print();//.writeAsText("/Users/bousavong/Desktop/test/text/DEB12/bidirection.txt");//.print();
                }
        env.execute("Word Count Example");
        tEnd = System.currentTimeMillis();
        elapsedSeconds = (tEnd - tStart) / 1000.0;
        System.out.println("Time:"+elapsedSeconds);
        System.out.println("Finish");
    }
}
