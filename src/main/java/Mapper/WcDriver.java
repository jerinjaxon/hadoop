package Mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WcDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Path input_path = new Path(args[0]);
        Path Output_Path = new Path(args[1]);

        Configuration config = new Configuration();
        Job job = new Job(config);
//        job.setMapperClass(WcMapper.class);
        job.setMapperClass(WcAnimalMapper.class);
        job.setReducerClass(WcReducer.class);
        job.setJarByClass(WcDriver.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job,input_path);
        FileOutputFormat.setOutputPath(job,Output_Path);

        if(job.waitForCompletion(true))
        {
            System.out.println("completed successfully");
        }
    }
}
