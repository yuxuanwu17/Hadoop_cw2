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

        // The method is to extract useful word
        public String deleteNotation(String words) {
            String regEx = "[\\W[0-9]]";
            Pattern p = Pattern.compile(regEx);
            Matcher matcher = p.matcher(words);
            return matcher.replaceAll("").trim();
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            ArrayList<String> bigrams = new ArrayList<String>();
            String[] single_word = value.toString().split("\\s+");

            // create and fill the bigram arraylist
            // the bigram has number of (single_word -1) since it has two words
            for (int i = 0; i < single_word.length - 1; i++) {
                single_word[i] = deleteNotation(single_word[i]);
                single_word[i + 1] = deleteNotation(single_word[i + 1]);
                if (!(single_word[i].isEmpty()) && !(single_word[i + 1].isEmpty())) {
                    bigrams.add(single_word[i] + " " + single_word[i + 1]);
                }

            }
            for (String token : bigrams) {
//                System.out.print(token);
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