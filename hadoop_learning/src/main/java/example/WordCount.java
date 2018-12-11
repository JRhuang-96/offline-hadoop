package example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import outline_calcul.StringUtils;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 Hadoop最新版本的MapReduce Release 0.20.0的API包括了一个全新的Mapreduce JAVA API，有时候也称为上下文对象。
 新的API类型上不兼容以前的API，所以，以前的应用程序需要重写才能使新的API发挥其作用 。
 新的API和旧的API之间有下面几个明显的区别。
 新的API倾向于使用抽象类，而不是接口，因为这更容易扩展。例如，你可以添加一个方法(用默认的实现)到一个抽象类而不需修改类之前的实现方法。在新的API中，Mapper和Reducer是抽象类。
 新的API是在org.apache.hadoop.mapreduce包(和子包)中的。之前版本的API则是放在org.apache.hadoop.mapred中的。
 新的API广泛使用context object(上下文对象)，并允许用户代码与MapReduce系统进行通信。例如，MapContext基本上充当着JobConf的OutputCollector和Reporter的角色。
 新的API同时支持"推"和"拉"式的迭代。在这两个新老API中，键/值记录对被推mapper中，但除此之外，新的API允许把记录从map()方法中拉出，这也适用于reducer。"拉"式的一个有用的例子是分批处理记录，而不是一个接一个。
 新的API统一了配置。旧的API有一个特殊的JobConf对象用于作业配置，这是一个对于Hadoop通常的Configuration对象的扩展。
 在新的API中，这种区别没有了，所以作业配置通过Configuration来完成。作业控制的执行由Job类来负责，而不是JobClient，它在新的API中已经荡然无存。

 */
//这是新版API
public class WordCount{

   public static class TokenizerMapper extends Mapper<Object,Text, Text,IntWritable>{

       private static final IntWritable one = new IntWritable(1);
       private Text word = new Text();
       MultipleOutputs mos = null;

       @Override
       protected void setup(Context context) {
        mos = new  MultipleOutputs<>(context);

       }

       public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
           StringTokenizer tokenizer = new StringTokenizer(value.toString());
           while(tokenizer.hasMoreTokens()){
               word.set(tokenizer.nextToken());
               context.write(word,one);
           }

       }
   }

   public static class InSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
       private IntWritable result = new IntWritable();

       public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
           int sum = 0;
           for (IntWritable val : values) {
               sum += val.get();
           }
           result.set(sum);
           context.write(key, result);

       }
   }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job= Job.getInstance(conf,"wordCount");

        job.setJarByClass(WordCount.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(InSumReducer.class);
        job.setReducerClass(InSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //有些作业不需要进行归约进行处理，那么就可以设置reduce的数量为0来进行处理，这种情况下用户的作业运行速度相对较高，
        // map的输出会直接写入到 SetOutputPath(path)设置的输出目录，而不是作为中间结果写到本地。
        // 同时Hadoop框架在写入文件系统前并不对之进行排序。
        job.setNumReduceTasks(0);
//        避免生成默认的文件 ,
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);



        FileInputFormat.addInputPath(job, new Path("./tmp/tmpdata/learning.txt"));
        FileOutputFormat.setOutputPath(job, new Path("./"+ StringUtils.getFileName()));

        System.exit(job.waitForCompletion(true)?0:1);

    }
}
