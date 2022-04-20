package advanced.customwritable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class FlowTypeAndYearObject implements WritableComparable<FlowTypeAndYearObject> {

    private String flowType;
    private String year;

    public FlowTypeAndYearObject() {
    }

    public FlowTypeAndYearObject(String flowType, String year) {
        this.flowType = flowType;
        this.year = year;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getFlowType() {
        return flowType;
    }

    public void setFlowType(String flowType) {
        this.flowType = flowType;
    }


    @Override
    public int compareTo(FlowTypeAndYearObject o) {
        if (this.hashCode() < o.hashCode()) return -1;
        else if (this.hashCode() > o.hashCode()) return +1;
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(flowType);
        dataOutput.writeUTF(year);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        flowType = dataInput.readUTF();
        year = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FlowTypeAndYearObject that = (FlowTypeAndYearObject) o;
        return Objects.equals(flowType, that.flowType) && Objects.equals(year, that.year);
    }

    @Override
    public int hashCode() {
        return Objects.hash(flowType, year);
    }

    @Override
    public String toString() {
        return flowType + "     " + year + "    ";
    }
}
