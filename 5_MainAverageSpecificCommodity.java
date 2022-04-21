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

public class 5_MainAverageSpecificCommodity {

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
        j.setJarByClass(5_MainAverageSpecificCommodity.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(ReduceForAverage.class);
//        j.setCombinerClass(CombineForAverage.class);

        // Definição de tipos de saida
        // map (Text, FireAvgTempWritable)
        j.setMapOutputKeyClass(PriceCommodity.class);
        j.setMapOutputValueClass(PerYearAvgCommodity.class);
        // reduce (Text, DoubleWritable)
        j.setOutputKeyClass(PriceCommodity.class);
        j.setOutputValueClass(DoubleWritable.class);

        // definicao de arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    public static class MapForAverage extends Mapper<LongWritable, Text, PriceCommodity, PerYearAvgCommodity> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            // Obtendo a linha para processamento
            String linha = value.toString();

            // Quebrando em campos
            String campos[] = linha.split(";");

            if(!campos[1].equals("year") && (!campos[1].isEmpty()) && (!campos[5].isEmpty())) {
                if (campos[0].equals("Brazil") && campos[4].equals("Export")) {
                    double price = Double.parseDouble(campos[5]);
                    String year = campos[1];
                    String unit_type = campos[7];
                    String category = campos[9];

                    con.write(new PriceCommodity(unit_type, year, category), new PerYearAvgCommodity(1, price));
                }
            }
        }
    }

    public static class ReduceForAverage extends Reducer<PriceCommodity, PerYearAvgCommodity,
            PriceCommodity, DoubleWritable> {
        public void reduce(PriceCommodity key, Iterable<PerYearAvgCommodity> values, Context con)
                throws IOException, InterruptedException {
            // Receber (chave, lista de valores)
            // (chave="media", [.............])
            // cada valor é um objeto (n, soma)
            // laço de repetição
            //     somar os Ns e somar as somas
            int nTotal = 0;
            double somaTotal = 0.0;
            for(PerYearAvgCommodity o : values){
                nTotal += o.getNumber();
                somaTotal += o.getSum();
            }
            // Media = soma das somas / soma dos Ns
            double media = somaTotal / nTotal;
            // salvando o resultado-\"!''1\Azsawq2
            con.write(key, new DoubleWritable(media));

        }
    }

}
