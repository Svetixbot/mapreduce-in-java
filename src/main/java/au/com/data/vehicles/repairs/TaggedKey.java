package au.com.data.vehicles.repairs;

import com.google.common.collect.ComparisonChain;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.apache.hadoop.io.WritableUtils.*;


public class TaggedKey implements WritableComparable<TaggedKey> {

    public enum Tag {

        REPAIR,
        VEHICLE;

        public static final String KEY_SEPARATOR = "::";

    }

    public TaggedKey(String vehicleType, Tag tag) {
        this.vehicleType = vehicleType;
        this.tag = tag;
    }

    public TaggedKey() {
    }

    public String getVehicleType() {
        return vehicleType;
    }

    public void setVehicleType(String vehicleType) {
        this.vehicleType = vehicleType;
    }

    public Tag getTag() {
        return tag;
    }

    public void setTag(Tag tag) {
        this.tag = tag;
    }

    private String vehicleType;
    private Tag tag;

    @Override
    public int compareTo(TaggedKey o) {
        return ComparisonChain.start()
                .compare(this.vehicleType,o.getVehicleType())
                .compare(this.getTag(),o.getTag())
                .result();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        writeString(out, vehicleType);
        writeEnum(out, tag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        vehicleType = readString(in);
        tag = readEnum(in,Tag.class);
    }
}
