import java.io.IOException;
import java.util.StringTokenizer;

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

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        /**
         *
         * @param key k1 行偏移量
         * @param value v1 每一行的文本数据
         * @param context 表示上下文的对象，桥梁，将上下给联系起来
         * @throws IOException
         * @throws InterruptedException
         * 这里是为了改写map的function，将k1，v1 转换为k2，v2
         */

        /*
        * 如何将k1 v1 转为k2， v2
        * k1      v1
        * 0       hello, world, hadoop 之类的文字数据
        * --------------------------------
        * k2            v2
        * hello           1
        * world           1
        * hadop           1
        * */

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //1: 将一行的文本数据进行拆分
            //2：遍历数组，组装k2和v2
            //3：将k2 和v2 写入下文（context）

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        /**
         * reduce 方法的作用： 将新的k2 和 V2 转化为k3和v3 ，将k3和v3写入context中
         * 参数：
         *  key: 新的k2
         *  values： 新的集合V2
         *  context：表示上下文的对象
         *  --------------
         *  如何将新的k2和v2转化为k3和v3
         *  hello <1,1,1>
         *  world <1,1>
         *  hadoop <1>
         *  --------------
         *  k3 and v3
         *  hell0   3
         *  world   2
         *  hadoop  1
         *  --------------
         * 将集合里面的数字全部相加就成为了我们的v3         *
         */
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            // 1： 遍历集合，将集合中的数字相加，得到我们的v3

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            //2: 将k3和v3写入上下文中
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        /**
         * 定义主类，并且提交脚本任务到hadoop中进行运行
         */
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count"); //创建一个job的任务对象

        //2： 配置job任务的8个步骤
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}