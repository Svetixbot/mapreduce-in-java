package au.com.data.wordscount;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordsCount extends Configured implements Tool
{
    private static final Logger logger = LoggerFactory.getLogger(WordsCount.class);

    public static class MapClass extends Mapper<LongWritable, Text, Text, LongWritable>
    {
        private final static LongWritable one = new LongWritable(1);
        private Text word = new Text();

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context)
                throws IOException, InterruptedException
        {
            super.setup(context);
        }

        @Override
        public void map(LongWritable key, Text text, Mapper<LongWritable, Text, Text, LongWritable>.Context
                context) throws IOException, InterruptedException
        {
            String regex = context.getConfiguration().get("wordcount.regex", "[A-Za-z']+");
            Pattern p = Pattern.compile(regex);
            Matcher m = p.matcher(text.toString());

            while(m.find()){
                word.set(m.group().toLowerCase());
                context.write(word, one);
            }
        }
    }

    public static class ReduceClass extends Reducer<Text, LongWritable, Text, LongWritable>
    {
      public ReduceClass(){}

        @Override
        protected void setup(Reducer<Text, LongWritable, Text, LongWritable>.Context context)
                throws IOException, InterruptedException
        {
            super.setup(context);
        }

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException
        {
            long sum = 0;
            for(LongWritable val : values){
                sum += val.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    @Override
    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException
    {
        logger.warn("Starting map/reduce job");
        Configuration conf = new Configuration();
        Job job = new Job(conf);

        job.setJarByClass(WordsCount.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        Path outPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outPath);

        //this will delete the path before it starts, good for debugging
        outPath.getFileSystem(conf).delete(outPath, true);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new WordsCount(), args);
        System.exit(res);
    }

}


