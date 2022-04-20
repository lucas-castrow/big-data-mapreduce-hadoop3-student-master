package advanced.customwritable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class MaxMinMeanWritable implements WritableComparable<MaxMinMeanWritable> {

    private String unit_type;
    private String year;
    public MaxMinMeanWritable() {
    }

    public MaxMinMeanWritable(String unit_type, String year) {
        this.unit_type = unit_type;
        this.year = year;
    }


    @Override
    public int compareTo(MaxMinMeanWritable o) {
        if (this.hashCode() < o.hashCode()) return -1;
        else if (this.hashCode() > o.hashCode()) return +1;
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(unit_type);
        dataOutput.writeUTF(year);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        unit_type = dataInput.readUTF();
        year = dataInput.readUTF();
    }


    @Override
    public String toString() {
        return  year + "   " +unit_type;
    }
}
