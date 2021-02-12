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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.io.WritableComparable;

public class Task1 {

    // Map class
    public static class Top_10_Bigram_Mapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();


        //Map function
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Split the input text and split("(\\s+)") means one or more blank character
            // Delete the notations of words and split it into two words such as "I'm"
            String[] oneword = value.toString().split("[\\s+]|[\\'+]|[\\W[0-9]]");


            // Traverse each element object using a for loop and combine every two words
            for (int i = 0; i < oneword.length - 1; i++) {
                if (!oneword[i].equals("") && !oneword[i + 1].equals("")) {
                    word.set(oneword[i] + "-" + oneword[i + 1]);
                    context.write(word, one);
                }

            }


        }
    }

    // Output the top 10 common bigrams
    public static class Top_10_Bigram_Mapper2 extends
            Mapper<Object, Text, IntWritable, Text> {

        int temp = 0;
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString());
            IntWritable m = new IntWritable(Integer.parseInt(itr.nextToken()));
            Text n = new Text(itr.nextToken());

            if (temp < 10) {
                context.write(m, n);
                temp++;
            }
        }

    }


    // Reduce class
    public static class Top_10_Bigram_Reducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        // private IntWritable result = new IntWritable();

        // Reduce function
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }


    // Define this DecreasingComparator class to sort in decreasing order
    private static class IntWritableDecreasingComparator extends
            IntWritable.Comparator {

        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }


    // Main function
    public static void main(String[] args) throws Exception {

        // Create 1st configuration and 1st job – in order to separate all words in pg100.txt to pair of adjacent words
        Configuration conf = new Configuration();
        Path input = new Path(args[0]);
        Path temp1 = new Path("temp1");
        Path temp2 = new Path("temp2");
        Path output = new Path(args[1]);
        Job job = new Job(conf, "Task1");



        job.setJarByClass(Task1.class);

        {
            job.setMapperClass(Top_10_Bigram_Mapper.class); // Set my Mapper class
            job.setCombinerClass(Top_10_Bigram_Reducer.class); // Set my Combiner class
            job.setReducerClass(Top_10_Bigram_Reducer.class); // Set my Reducer class
            job.setOutputKeyClass(Text.class); // Set the key class
            job.setOutputValueClass(IntWritable.class); // Set the value class
            FileInputFormat.addInputPath(job, input); // Set the input path
            FileOutputFormat.setOutputPath(job, temp1);//set the output path
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            // Sort the occurrences of the bigrams
            if (job.waitForCompletion(true)) {

                Job sortJob = new Job(conf, "sort");
                sortJob.setJarByClass(Task1.class);
                FileInputFormat.addInputPath(sortJob, temp1);
                sortJob.setInputFormatClass(SequenceFileInputFormat.class); // Set the SequenceFileInputFormat class
                sortJob.setMapperClass(InverseMapper.class); // Set the InverseMapper class
                sortJob.setNumReduceTasks(1);
                FileOutputFormat.setOutputPath(sortJob, temp2);
                sortJob.setOutputKeyClass(IntWritable.class);
                sortJob.setOutputValueClass(Text.class);
                sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class); // Use a decreasing order


                if (sortJob.waitForCompletion(true)) {
                    //output the top10 result
                    Job top10Job = new Job(conf, "top10");
                    top10Job.setJarByClass(Task1.class);
                    FileInputFormat.addInputPath(top10Job, temp2);
                    top10Job.setMapperClass(Top_10_Bigram_Mapper2.class);
                    top10Job.setNumReduceTasks(1);
                    FileOutputFormat.setOutputPath(top10Job, output);
                    top10Job.setOutputKeyClass(IntWritable.class);
                    top10Job.setOutputValueClass(Text.class);
                    System.exit(top10Job.waitForCompletion(true) ? 0 : 1);
                }

            }

        }
    }
}




