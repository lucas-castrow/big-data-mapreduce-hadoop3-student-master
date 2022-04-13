package advanced.customwritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class NumberOfTransactionsBrazil implements WritableComparable<NumberOfTransactionsBrazil> {
    /**
     * Todo writable precisa ser um Java BEAN!
     * 1- Construtor vazio (OK)
     * 2- Gets e sets (OK)
     * 3- Comparação entre objetos (OK)
     * 4- Atributos privados (OK)
     */

    private String pais;
    private int valor;

    public NumberOfTransactionsBrazil() {
    }

    public NumberOfTransactionsBrazil(String pais, int valor) {
        this.pais = pais;
        this.valor = valor;
    }

    public String getPais() {
        return pais;
    }

    public void setPais(String pais) {
        this.pais = pais;
    }

    public int getValor() {
        return valor;
    }

    public void setValor(int valor) {
        this.valor = valor;
    }

    @Override
    public int compareTo(NumberOfTransactionsBrazil o) {
        if(this.hashCode() > o.hashCode()){
            return +1;
        }else if(this.hashCode() < o.hashCode()){
            return -1;
        }
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBytes(pais); // MUITO CUIDADO COM A ORDEM!
        dataOutput.writeDouble(valor);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        pais = dataInput.toString(); // DEVE SEGUIR A MESMA ORDEM DA ESCRITA!
        valor = dataInput.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NumberOfTransactionsBrazil that = (NumberOfTransactionsBrazil) o;
        return pais == that.pais;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pais, valor);
    }

}
