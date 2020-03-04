package com.sample.mr;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.StringTokenizer;

import com.google.gson.Gson;
import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query {

    public static class KMeansMapper
            extends Mapper<Object, Text, IntWritable, Text>{
        Gson gson = new Gson();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String initial_points_str = conf.get("initial_points");
            float[][] initial_points =  gson.fromJson(initial_points_str,float[][].class);
            String[] data = value.toString().split(",");
            float[] points= new float[data.length];
            for (int i =0;i<data.length;i++){
                points[i]=Float.parseFloat(data[i]);
            }
            Pair<Integer, Float> p = closest(points,initial_points);
            int index=p.getKey();
            float distance=p.getValue();
            String dist_str=String.valueOf(distance);
            context.write(new IntWritable(index),new Text(value+","+dist_str+",1"));
        }
    }
    public static class KMeansCombiner
            extends Reducer<IntWritable,Text,IntWritable,Text> {
        private Text result = new Text();

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            float number_of_points=0;
            float[] new_center = new float[] {0,0};
            float inertia = 0;
            for (Text val : values) {
                String[] data = val.toString().split(",");
                new_center[0]+=Float.valueOf(data[0]);
                new_center[1]+=Float.valueOf(data[1]);
                inertia+=Float.valueOf(data[2]);
                number_of_points+=1;
            }
            String center=String.valueOf(new_center[0])+","+String.valueOf(new_center[1]);
            result.set(center+","+String.valueOf(inertia)+","+String.valueOf(number_of_points));
            context.write(key, result);
        }
    }

    public static class KMeansReducer
            extends Reducer<IntWritable,Text,IntWritable,Text> {
        private Text result = new Text();

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            float number_of_points=0;
            float[] new_center = new float[] {0,0};
            float inertia = 0;
            for (Text val : values) {
                String[] data = val.toString().split(",");
                new_center[0]+=Float.valueOf(data[0]);
                new_center[1]+=Float.valueOf(data[1]);
                inertia+=Float.valueOf(data[2]);
                number_of_points+=Float.valueOf(data[3]);
            }
            String center=String.valueOf(new_center[0]/number_of_points)+","+String.valueOf(new_center[1]/number_of_points);
            result.set(center+","+String.valueOf(inertia));
            context.write(key, result);
        }
    }

    public static Pair<Integer, Float> closest(float[] point, float[][] cluster_centers){
        float distance=Float.POSITIVE_INFINITY;
        int index = -1;
        for (int i = 0; i < cluster_centers.length; i++) {
            float temp_distance=distBetweenPointsSq(cluster_centers[i],point);
            if (distance>temp_distance){
                distance=temp_distance;
                index=i;
            }
        }
        return new Pair<Integer, Float>(index, distance);
    }
    public static float distBetweenPointsSq(
            float[] point1,
            float[] point2) {
        float sums=0;
        for (int i = 0; i < point1.length; i++){
            sums+=Math.pow((point1[i]-point2[i]),2);
        }
        return sums;
    }

    public static float[][] random_init(int number_of_points,int max_val){
        float[][] init= new float[number_of_points][2];
        for (int i=0;i<number_of_points;i++){
            init[i][0]=(float) (Math.random()*max_val);
            init[i][1]=(float) (Math.random()*max_val);
        }
        return init;
    }
    public static void main(String[] args) throws Exception {
        Gson gson = new Gson();
        int number_of_iterations=10;
        Configuration conf = new Configuration();
        float[][] initial_points={{0,0},{100,100}};//random_init(10,100);
        String initial_points_str=gson.toJson(initial_points);
        conf.set("initial_points", initial_points_str);
        Job job = Job.getInstance(conf, "k_means");
        job.setJarByClass(Query.class);
        job.setMapperClass(KMeansMapper.class);
        job.setCombinerClass(KMeansCombiner.class);
        job.setReducerClass(KMeansReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}