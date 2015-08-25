package au.com.data.vehicles.repairs;

import org.apache.hadoop.io.Text;
import static java.lang.Math.abs;

public class Partitioner extends org.apache.hadoop.mapreduce.Partitioner<TaggedKey,Text>{
    @Override
    public int getPartition(TaggedKey taggedKey, Text text, int numberOfReducers) {
        return abs(taggedKey.getVehicleType().hashCode()) % numberOfReducers;
    }
}
