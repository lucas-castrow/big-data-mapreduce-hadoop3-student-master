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

public class CommodityValues {

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
        Job j = new Job(c, "media");

        // Registro de classes
        j.setJarByClass(CommodityValues.class);
        j.setMapperClass(MapForAverage.class);
        //j.setReducerClass(ReduceForAverage.class);
        j.setCombinerClass(CombineForAverage.class);

        // Definição de tipos de saida
        // map (Text, FireAvgTempWritable)
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(CommodityAvgWritable.class);
        // reduce (Text, DoubleWritable)
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        // definicao de arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    public static class MapForAverage extends Mapper<LongWritable, Text, Text, CommodityAvgWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            // Obtendo a linha para processamento
            String linha = value.toString();

            // Quebrando em campos
            String campos[] = linha.split(";");

            // obtendo a temperatura
            double temperatura = Double.parseDouble(campos[8]);
            String mes = campos[1];

            // emitir (chave, valor) -> ("media", (n=1, sum=temperatura))
            con.write(new Text("media"), new CommodityAvgWritable(1, temperatura));
            con.write(new Text(mes), new CommodityAvgWritable(1, temperatura));

        }
    }

    public static class CombineForAverage extends Reducer<Text, CommodityAvgWritable,
            Text, CommodityAvgWritable>{
        public void reduce(Text key, Iterable<CommodityAvgWritable> values, Context con)
                throws IOException, InterruptedException {
            // O objetivo deste combiner é SOMAR os Ns e as SOMAS parciais
            int totalN = 0;
            double totalSoma = 0.0;
            for(CommodityAvgWritable o : values){
                totalN += o.getN();
                totalSoma += o.getSoma();
            }

            // enviando do combiner para o sort/shuffle
            con.write(key, new CommodityAvgWritable(totalN, totalSoma));

        }
    }


    public static class ReduceForAverage extends Reducer<Text, CommodityAvgWritable,
            Text, DoubleWritable> {
        public void reduce(Text key, Iterable<CommodityAvgWritable> values, Context con)
                throws IOException, InterruptedException {
            // Receber (chave, lista de valores)
            // (chave="media", [.............])
            // cada valor é um objeto (n, soma)
            // laço de repetição
            //     somar os Ns e somar as somas
            int nTotal = 0;
            double somaTotal = 0.0;
            for(CommodityAvgWritable o : values){
                nTotal += o.getN();
                somaTotal += o.getSoma();
            }
            // Media = soma das somas / soma dos Ns
            double media = somaTotal / nTotal;
            // salvando o resultado
            con.write(key, new DoubleWritable(media));

        }
    }

}
