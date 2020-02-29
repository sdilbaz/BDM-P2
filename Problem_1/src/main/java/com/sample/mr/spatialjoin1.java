package com.sample.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class spatialjoin1 {

    public static class SpatialMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String[] data = value.toString().split(",");
            context.write(new Text(),new Text(value.toString()));
        }
    }


    public static class SpatialReducer extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> P = new ArrayList<String>();
            List<String> R = new ArrayList<String>();

            for (Text value:values) {
                String str = value.toString();
                if (str.split(",").length != 2) {
                    R.add(str);
                } else {
                    P.add(str);
                }
            }
            for (String r:R) {
                String[] temp_r = r.split(",");
                String name = temp_r[0];
                int X_1 = Integer.valueOf(temp_r[1]);
                int Y_1 = Integer.valueOf(temp_r[2]);
                int X_2 = Integer.valueOf(temp_r[3]);
                int Y_2 = Integer.valueOf(temp_r[4]);
                for (String p:P) {
                    String[] temp_p = p.split(",");
                    int x = Integer.valueOf(temp_p[0]);
                    int y = Integer.valueOf(temp_p[1]);
                    if (x>=X_1 && x<=X_2 && y>=Y_1 && y<=Y_2) {
                        context.write(new Text(name),new Text(p));
                    }

                }
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "spatialJoin");
        job.setJarByClass(spatialjoin1.class);
        job.setMapperClass(SpatialMapper.class);
        job.setReducerClass(SpatialReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
