import java.io.IOException;
import java.util.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount_torture {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "torture count");
        job.setJarByClass(WordCount_torture.class); //My class name is 'word count'

        job.setMapperClass(TokenizerMapper.class); // Set my Mapper class
        job.setReducerClass(IntSumReducer.class); // Set my Reducer class

        job.setOutputKeyClass(Text.class);  // Set the key class for my output class
        job.setOutputValueClass(Text.class); // Set the value class for the output data

        job.setInputFormatClass(TextInputFormat.class);// Set input format for this job
        job.setOutputFormatClass(TextOutputFormat.class); // Set output format for this job

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class TokenizerMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

            String line[] = value.toString().split("\\n{1,}");
            ArrayList<String> results = new ArrayList<>();

            for (int i = 0; i < line.length; i++) {
                if (line[i].contains("torture")) {
                    results.add(line[i]);
                }
            }


            for (String result : results) {
                word.set(result);
                context.write(word, new Text(" "));
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, Text> {
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            context.write(new Text(key), new Text(" "));

        }
    }


}
