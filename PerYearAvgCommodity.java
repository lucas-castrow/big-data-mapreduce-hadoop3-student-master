package advanced.customwritable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class PerYearAvgCommodity implements WritableComparable<PerYearAvgCommodity> {
    /**
     * Todo writable precisa ser um Java BEAN!
     * 1- Construtor vazio (OK)
     * 2- Gets e sets (OK)
     * 3- Comparação entre objetos (OK)
     * 4- Atributos privados (OK)
     */

    private int number;
    private double sum;

    public PerYearAvgCommodity() {
    }

    public PerYearAvgCommodity(int number, double sum) {
        this.number = number;
        this.sum = sum;
    }

    @Override
    public int compareTo(PerYearAvgCommodity o) {
        if(this.hashCode() > o.hashCode()){
            return +1;
        }else if(this.hashCode() < o.hashCode()){
            return -1;
        }
        return 0;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(number); // MUITO CUIDADO COM A ORDEM!
        dataOutput.writeDouble(sum);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        number = dataInput.readInt(); // DEVE SEGUIR A MESMA ORDEM DA ESCRITA!
        sum = dataInput.readDouble();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PerYearAvgCommodity that = (PerYearAvgCommodity) o;
        return number == that.number && Double.compare(that.sum, sum) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(number, sum);
    }
}
