import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount_torture {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "torture count");
        job.setJarByClass(WordCount_torture.class); //My class name is 'word count'
        job.setMapperClass(TokenizerMapper.class); // Set my Mapper class

        job.setReducerClass(TokenizerMapper.IntSumReducer.class); // Set my Reducer class
        job.setOutputKeyClass(Text.class); // Set the key class for my output class
        job.setOutputValueClass(IntWritable.class); // Set the value class for the output data

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class TokenizerMapper
            extends Mapper<LongWritable, Text, Text, Text> {


        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

//            String line = value.toString();
            String line[] = value.toString().split("\\n{1,}");
//            String tmp[] = line.split("\\n{1,}");

            ArrayList<String> results = new ArrayList<String>();

            for (int i = 0; i < line.length; i++) {
                if (line[i].contains("torture")) {
                    results.add(line[i]);
                }
            }
//            for (int i = 0; i < tmp.length; i++) {
//                if(tmp[i].contains("torture")){
//                    results.add(line);
//                }
//            }

            for (String result : results) {
                word.set(result);
                context.write(word,new Text(" "));
            }
        }

        public static class IntSumReducer
                extends Reducer<Text, IntWritable, Text, IntWritable> {
            private IntWritable result = new IntWritable();

            public void reduce(Text key, Iterable<IntWritable> values,
                               Context context
            ) throws IOException, InterruptedException {

                int sum = 0;
                for (IntWritable val : values) {
                    sum += val.get();
                }
                result.set(sum);
                context.write(key, result);
            }
        }


    }
}