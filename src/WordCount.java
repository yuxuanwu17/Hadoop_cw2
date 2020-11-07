import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bigram count");
        job.setJarByClass(WordCount.class); //My class name is 'word count'
        job.setMapperClass(TokenizerMapper.class); // Set my Mapper class
//        job.setCombinerClass(TokenizerMapper.IntSumReducer.class); // Set my Combiner class
        job.setReducerClass(TokenizerMapper.IntSumReducer.class); // Set my Reducer class
        job.setOutputKeyClass(Text.class); // Set the key class for my output class
        job.setOutputValueClass(IntWritable.class); // Set the value class for the output data

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            ArrayList<String> bigrams = new ArrayList<String>();
            String[] single_word = value.toString().split("\\s+");
            for (String token : single_word) {
                word.set(token);
                context.write(word, one);
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