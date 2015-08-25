package au.com.data.vehicles.count;

import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
import org.apache.hadoop.io.Text;


import java.io.IOException;


public class VehiclesCount extends Configured implements Tool {

    private static final Logger logger = LoggerFactory.getLogger(VehiclesCount.class);

    public static class MapClass extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String carType = value.toString().split(",")[0];
            context.write(new Text(carType), new LongWritable(1));
        }
    }

    public static class ReduceClass extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            context.write(new Text(key), new LongWritable(Iterables.size(values)));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        logger.warn("Starting map/reduce job");
        Configuration conf = new Configuration();
        Job job = new Job(conf);

        job.setJarByClass(VehiclesCount.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        Path outPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outPath);

        //this will delete the path before it starts, good for debugging
        outPath.getFileSystem(conf).delete(outPath, true);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new VehiclesCount(), args);
        System.exit(res);
    }
}
