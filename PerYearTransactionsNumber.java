package advanced.customwritable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class PerYearTransactionsNumber implements WritableComparable<PerYearTransactionsNumber> {

    private int contagem;

    public PerYearTransactionsNumber() {
    }

    public PerYearTransactionsNumber(int contagem) {
        this.contagem = contagem;
    }

    public int getContagem() {
        return contagem;
    }

    public void setContagem(int contagem) {
        this.contagem = contagem;
    }

    @Override
    public int compareTo(PerYearTransactionsNumber o) {
        if (this.hashCode() < o.hashCode()) return -1;
        else if (this.hashCode() > o.hashCode()) return +1;
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(contagem);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        contagem = dataInput.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PerYearTransactionsNumber that = (PerYearTransactionsNumber) o;
        return Integer.compare(that.contagem, contagem) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(contagem);
    }
}
