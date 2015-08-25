package au.com.data.vehicles.repairs;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import sun.awt.AppContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static au.com.data.vehicles.repairs.TaggedKey.Tag.*;

public class DenormalizingReducer extends Reducer<TaggedKey, Text, Text, Text> {

    protected List<Text> repairs;

    @Override
    protected void reduce(TaggedKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        repairs = new ArrayList<Text>();

        if (key.getTag().equals(REPAIR)) {
            while (values.iterator().hasNext()) {
                repairs.add(values.iterator().next());
                context.write(new Text(key.getTag() + ":" + key.getVehicleType()),values.iterator().next());
            }
        }

        if (key.getTag().equals(VEHICLE)) {
            while (values.iterator().hasNext()) {
                join(values.iterator().next(), context);
            }
        }
    }

    private void join(Text vehicle, Context context) throws IOException, InterruptedException {
        for (Text repair : repairs) {
            context.write(vehicle, repair);
        }
    }
}
