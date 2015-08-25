package au.com.data.vehicles.repairs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath;

public class VehiclesWithRepairs extends Configured implements Tool {

    private static final Logger logger = LoggerFactory.getLogger(VehiclesWithRepairs.class);

    @Override
    public int run(String[] args) throws Exception {
        logger.warn("Starting map/reduce job");
        Configuration conf = new Configuration();
        Job job = new Job(conf);

        job.setJarByClass(this.getClass());

        addInputPath(job, getPath(args[0]), TextInputFormat.class, TaggedVehicleMapper.class);
        addInputPath(job, getPath(args[1]), TextInputFormat.class, TaggedRepairMapper.class);

        Path outPath = getPath(args[2]);

        FileOutputFormat.setOutputPath(job, outPath);
        outPath.getFileSystem(conf).delete(outPath, true);

        job.setMapOutputKeyClass(TaggedKey.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(DenormalizingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setSortComparatorClass(SortComparator.class);
        job.setGroupingComparatorClass(GroupingComparator.class);
        job.setPartitionerClass(Partitioner.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    protected Path getPath(String path){
        return new Path(path);
    }

    public static void main(String[] args) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new VehiclesWithRepairs(), args);
        System.exit(res);
    }
}
