package au.com.data.vehicles.repairs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TaggedRepairMapper extends Mapper<LongWritable, Text, TaggedKey, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(",");
        String vehicleType = values[0];

        context.write(new TaggedKey(vehicleType.trim().toUpperCase(), TaggedKey.Tag.REPAIR),
                new Text(value));
    }
}
