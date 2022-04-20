package tde1.ex7;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class BaseQtdWritable implements WritableComparable<BaseQtdWritable> {
    private double qtd;
    private String content;

    public BaseQtdWritable() {
    }

    public BaseQtdWritable(double qtd, String content) {
        this.qtd = qtd;
        this.content = content;
    }

    public double getQtd() {
        return qtd;
    }

    public void setQtd(double qtd) {
        this.qtd = qtd;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BaseQtdWritable that = (BaseQtdWritable) o;
        return qtd == that.qtd && Objects.equals(content, that.content);
    }

    @Override
    public int hashCode() {
        return Objects.hash(qtd, content);
    }

    @Override
    public int compareTo(BaseQtdWritable o) {
        if (this.hashCode() < o.hashCode()) return -1;
        if (this.hashCode() > o.hashCode()) return +1;
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(qtd);
        dataOutput.writeUTF(content);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        qtd = dataInput.readDouble();
        content = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return "BaseQtdWritable{" +
                "qtd=" + qtd +
                ", content='" + content + '\'' +
                '}';
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return new BaseQtdWritable(this.qtd, this.content);
    }
}