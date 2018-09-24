package Mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WcAnimalMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String st = value.toString().trim();
        String[] animals = st.split(",");
        for (String ani:animals)
        {
          context.write(new Text(ani),new IntWritable(1));
        }
    }
}
