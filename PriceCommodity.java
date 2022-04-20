package advanced.customwritable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class PriceCommodity implements WritableComparable<PriceCommodity> {
    /**
     * Todo writable precisa ser um Java BEAN!
     * 1- Construtor vazio (OK)
     * 2- Gets e sets (OK)
     * 3- Comparação entre objetos (OK)
     * 4- Atributos privados (OK)
     */
    private String unit_type;
    private String year;
    private String category;

    public PriceCommodity() {
    }


    public PriceCommodity(String unit_type, String year, String category) {
        this.unit_type = unit_type;
        this.year = year;
        this.category = category;
    }

    public String getUnit_type() {
        return unit_type;
    }

    public void setUnit_type(String unit_type) {
        this.unit_type = unit_type;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    @Override
    public int compareTo(PriceCommodity o) {
        if(this.hashCode() > o.hashCode()){
            return +1;
        }else if(this.hashCode() < o.hashCode()){
            return -1;
        }
        return 0;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(unit_type);
        dataOutput.writeUTF(year);
        dataOutput.writeUTF(category);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        unit_type = dataInput.readUTF();
        year = dataInput.readUTF();
        category = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PriceCommodity)) return false;
        PriceCommodity that = (PriceCommodity) o;
        return unit_type.equals(that.unit_type) && year.equals(that.year) && category.equals(that.category);
    }

    @Override
    public int hashCode() {
        return Objects.hash(unit_type, year, category);
    }

    @Override
    public String toString() {
        return
                year + "    " +
                        unit_type + "   "+
                        category + "    ";
    }



}
