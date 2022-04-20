package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class TransactionsPerYear {

    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "ano");

        // Registro de classes
        j.setJarByClass(TransactionsPerYear.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(ReduceForAverage.class);
//        j.setCombinerClass(CombineForAverage.class);

        // Definição de tipos de saida
        // map (Text, FireAvgTempWritable)
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(PerYearTransactionsNumber.class);
        // reduce (Text, DoubleWritable)
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        // definicao de arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    public static class MapForAverage extends Mapper<LongWritable, Text, Text, PerYearTransactionsNumber> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            // Obtendo a linha para processamento
            String linha = value.toString();

            // Quebrando em campos
            String campos[] = linha.split(";");

            // obtendo a temperatura
            String ano = campos[1];
            if(!campos[1].equals("year")) {
                // emitir (chave, valor) -> ("media", (n=1, sum=temperatura))
                con.write(new Text(ano), new PerYearTransactionsNumber(1));
            }

        }
    }


    public static class ReduceForAverage extends Reducer<Text, PerYearTransactionsNumber,
            Text, IntWritable> {
        public void reduce(Text key, Iterable<PerYearTransactionsNumber> values, Context con)
                throws IOException, InterruptedException {
            // Receber (chave, lista de valores)
            // (chave="media", [.............])
            // cada valor é um objeto (n, soma)
            // laço de repetição
            //     somar os Ns e somar as somas
            int nTotal = 0;
            for(PerYearTransactionsNumber o : values){
                nTotal += o.getContagem();
            }
            // Media = soma das somas / soma dos Ns
            con.write(key, new IntWritable(nTotal));

        }
    }

}
