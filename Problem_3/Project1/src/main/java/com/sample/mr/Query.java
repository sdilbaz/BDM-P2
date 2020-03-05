package com.sample.mr;

import org.json.simple.JSONObject;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

import com.google.gson.Gson;
import javafx.util.Pair;
import org.apache.commons.io.FileUtils;
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

    public static class InitMapper
            extends Mapper<Object, Text, IntWritable, Text>{
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String prob_str = conf.get("probability");
            String cents_str = conf.get("number_cents");
            int number_of_centers = Integer.parseInt(cents_str);
            float prob = Float.parseFloat(prob_str);
            if (Math.random()<prob){
                int index = (int) Math.floor(Math.random()*number_of_centers);
                context.write(new IntWritable(index),new Text(value));
            }
        }
    }

    public static class InitReducer
            extends Reducer<IntWritable,Text,IntWritable,Text> {
        private Text result = new Text();

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            float[] new_center = new float[] {0,0};
            float number_of_points=0;
            for (Text val : values) {
                String[] data = val.toString().split(",");
                new_center[0]+=Float.parseFloat(data[0]);
                new_center[1]+=Float.parseFloat(data[1]);
                number_of_points+=1;
            }
            String center= new_center[0]/number_of_points +","+ new_center[1]/number_of_points;
            result.set(center);
            context.write(key, result);
        }
    }

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
                new_center[0]+=Float.parseFloat(data[0]);
                new_center[1]+=Float.parseFloat(data[1]);
                inertia+=Float.parseFloat(data[2]);
                number_of_points+=Float.parseFloat(data[3]);
            }
            String center= new_center[0] +","+ new_center[1];
            result.set(center+","+ inertia +","+ number_of_points);
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
                new_center[0]+=Float.parseFloat(data[0]);
                new_center[1]+=Float.parseFloat(data[1]);
                inertia+=Float.parseFloat(data[2]);
                number_of_points+=Float.parseFloat(data[3]);
            }
            String center= (new_center[0] / number_of_points) +","+ (new_center[1] / number_of_points);
            result.set(center+","+ inertia+","+number_of_points);
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

    public static float[][] smart_init(String filter_prob, int number_of_points, String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
        clear_output();
        Configuration conf = new Configuration();
        String number_cents= String.valueOf(number_of_points);
        conf.set("probability", filter_prob);
        conf.set("number_cents", number_cents);
        Job job = Job.getInstance(conf, "k_means_init");
        job.setJarByClass(Query.class);
        job.setMapperClass(InitMapper.class);
        job.setReducerClass(InitReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);

        float[][] init = random_init(number_of_points,10000);
        String dir_path=System.getProperty("user.dir");

        File file = new File(dir_path+"/output/part-r-00000");

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String st;
            while ((st = br.readLine()) != null){
                String[] entries = st.split("\t");
                String[] center_str=entries[1].split(",");
                init[Integer.parseInt(entries[0])][0]=Float.parseFloat(center_str[0]);
                init[Integer.parseInt(entries[0])][1]=Float.parseFloat(center_str[1]);
            }
        } catch (IOException ignored) {
        }
        clear_output();
        return init;
    }

    public static void clear_output(){
        String dir_path=System.getProperty("user.dir");
        try {
            FileUtils.deleteDirectory(new File(dir_path+"/output"));
        } catch (IOException e) {
        }
    }

    public static JSONObject extract_centers() {
        JSONObject centers = new JSONObject();
        String dir_path=System.getProperty("user.dir");

        File file = new File(dir_path+"/output/part-r-00000");

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String st;
            while ((st = br.readLine()) != null){
                String[] entries = st.split("\t");
                String[] center_str=entries[1].split(",");
                float[] center = new float[2];
                center[0]=Float.parseFloat(center_str[0]);
                center[1]=Float.parseFloat(center_str[1]);

                centers.put(Integer.valueOf(entries[0]),center);
            }
        } catch (IOException ignored) {
        }
        return centers;
    }

    public static float extract_inertia() {
        float inertia=0;
        String dir_path=System.getProperty("user.dir");

        File file = new File(dir_path+"/output/part-r-00000");

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String st;
            while ((st = br.readLine()) != null){
                String[] entries = st.split("\t");
                String[] center_str=entries[1].split(",");
                inertia+=Float.parseFloat(center_str[2]);
            }
        } catch (IOException ignored) {
        }
        return inertia;
    }


    public static void main(String[] args) throws Exception {
        int number_of_centers=20;
        int number_of_iterations=10;
        float current_inertia;
        String filter_prob = "0.1";

        ArrayList<Float> inertias;
        inertias = new ArrayList<Float>();
        JSONObject centers;
        float[][] initial_points=smart_init(filter_prob,number_of_centers,args[0],args[1]);
        //{{(float) 4268.7476,(float) 4845.512}, {(float) 4234.1167,(float) 4829.2954}, {(float) 4255.073,(float) 4851.9497}, {(float) 4278.561,(float) 4826.274}, {(float) 4246.077,(float) 4856.6426}, {(float) 4282.9805,(float) 4844.3105}, {(float) 4292.4106,(float) 4840.826}, {(float) 4287.5,(float) 4843.4976}, {(float) 4265.252,(float) 4842.9917}, {(float) 4267.999,(float) 4849.925}, {(float) 4256.675,(float) 4837.21}, {(float) 4263.205,(float) 4839.4316}, {(float) 4276.2617,(float) 4836.1646}, {(float) 4284.362,(float) 4813.5244}, {(float) 4272.6904,(float) 4823.723}, {(float) 4285.831,(float) 4828.006}, {(float) 4262.362,(float) 4832.6406}, {(float) 4243.9717,(float) 4864.162}, {(float) 4271.266,(float) 4832.21}, {(float) 4271.984,(float) 4837.6997}};
        //smart_init(filter_prob,number_of_centers,args[0],args[1]);

        Gson gson = new Gson();
        clear_output();
        Configuration conf = new Configuration();
        String initial_points_str=gson.toJson(initial_points);
        for (int iteration=0;iteration<number_of_iterations;iteration++){
            System.out.println("Iteration "+ iteration);
            System.out.println("Cluster centers:");
            System.out.println(Arrays.deepToString(initial_points));
            conf.set("initial_points", initial_points_str);
            Job job = Job.getInstance(conf, "k_means_"+iteration);
            job.setJarByClass(Query.class);
            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class);
            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);

            // store centers and inertia
            centers = extract_centers();
            current_inertia=extract_inertia();
            if (inertias.size()!=0 && (inertias.get(inertias.size() - 1)==current_inertia)){
                System.out.println("k-means converged in "+ iteration +" iterations");
                break;
            }
            inertias.add(current_inertia);
            for (int center_id=0;center_id<number_of_centers;center_id++){
                Object temp=centers.get(center_id);
                if (temp!=null){
                    float[] current_center=(float[])temp;
                    initial_points[center_id][0]=current_center[0];
                    initial_points[center_id][1]=current_center[1];
                }
            }
            initial_points_str=gson.toJson(initial_points);
            if (!(iteration+1==number_of_iterations)){
                clear_output();
            }
            else {
                System.out.println("k-means did not converge after "+ number_of_iterations +" iterations");
            }
        }
        System.out.println("Cluster centers:");
        System.out.println(Arrays.deepToString(initial_points));
        System.out.println("Inertia over time:");
        System.out.println(Arrays.toString(inertias.toArray()));

    }
}