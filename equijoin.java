/**
 * Created by manideep on 11/20/16.
 */

/*
REFERENCES:
1) https://kishorer.in/2014/10/22/running-a-wordcount-mapreduce-example-in-hadoop-2-4-1-single-node-cluster-in-ubuntu-14-04-64-bit/
2) https://mrchief2015.wordpress.com/2015/02/09/compiling-and-debugging-hadoop-applications-with-intellij-idea-for-windows/
3) https://www.tutorialspoint.com/hadoop/hadoop_mapreduce.htm
 */

package com.dds.Equijoin;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.Iterator;

public class equijoin extends Configured implements Tool{

    public static class EquiJoinMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
    {
        private Text word = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
        {
            String line = value.toString();
            String[] splits = line.split(",");

            word.set(splits[1]);
            output.collect(word, new Text(line));

        }
    }

    public static class EquiJoinReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>
    {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
        {
            int i = 0;
            Set<Text> aSet1 = new HashSet<Text>();
            Set<Text> aSet2 = new HashSet<Text>();
            String s = "";


            while(values.hasNext()) {

                String l = values.next().toString();
                String[] lsplit = l.split(",");
                if(i == 0) {
                    s = lsplit[0];
                    aSet1.add(new Text(l));
                }
                else {
                    if(s.equalsIgnoreCase(lsplit[0])) {
                        aSet1.add(new Text(l));
                    }
                    else {
                        aSet2.add(new Text(l));
                    }
                }

                i++;
            }

            for(Text t1 : aSet1) {
                for(Text t2 : aSet2) {
                    String res = t1.toString() + ", " + t2.toString();
                    output.collect(new Text(res), null);
                }
            }
        }
    }


    public int run(String[] args) throws Exception
    {
        JobConf conf = new JobConf(getConf(), equijoin.class);
        conf.setJobName("equijoin");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(EquiJoinMapper.class);
        conf.setReducerClass(EquiJoinReducer.class);
        Path inp = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.addInputPath(conf, inp);
        FileOutputFormat.setOutputPath(conf, out);

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new equijoin(),args);
        System.exit(res);
    }
}