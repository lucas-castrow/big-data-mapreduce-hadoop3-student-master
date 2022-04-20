package tde1.ex7;

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
import java.util.LinkedList;

public class MostCommercializedCommodity{

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // arquivo auxiliar
        Path intermediate = new Path("./output/intermediate.tmp");

        // criacao do job e seu nome
        Job j = new Job(c, "media");

        // Registro de classes
        j.setJarByClass(MostCommercializedCommodity.class);
        j.setMapperClass(MapEtapaA.class);
        j.setReducerClass(ReduceEtapaA.class);

//        j.setCombinerClass(CombineForAverage.class);

        // Definição de tipos de saida
        // map (Text, FireAvgTempWritable)
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(DoubleWritable.class);
        // reduce (Text, DoubleWritable)
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        // definicao de arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, intermediate);

        // lanca o job e aguarda sua execucao
        //System.exit(j.waitForCompletion(true) ? 0 : 1);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, intermediate);

        // lanca o job e aguarda sua execucao
        // System.exit(j1.waitForCompletion(true) ? 0 : 1);
        if(!j.waitForCompletion(true)){
            System.err.println("Error with Job 1!");
            return;
        }

        Job j2 = new Job(c, "fasta-entropy-pt2");

        // registro das classes
        j2.setJarByClass(MostCommercializedCommodity.class);
        j2.setMapperClass(MapEtapaB.class);
        j2.setReducerClass(ReduceEtapaB.class);

        // definicao dos tipos de saida
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(BaseQtdWritable.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(DoubleWritable.class);

        // arquivos de entrada e saida
        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, output);

        if(!j2.waitForCompletion(true)){
            System.err.println("Error with Job 2!");
            return;
        }

    }
    public static class MapEtapaA extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // Obtendo a linha para processamento
            String linha = value.toString();

            // Quebrando em campos
            String campos[] = linha.split(";");


            String year = campos[1];
            if (year.equals("year")) return;
            if (!year.equals("2016"))return;
            String commodity_code = campos[2];
            String flow = campos[4];
            double amount = Double.parseDouble(campos[8]);
            String key_aux = commodity_code + "\t" + flow;
            con.write(new Text(key_aux), new DoubleWritable(amount));
        }
    }

    public static class ReduceEtapaA extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context con)
                throws IOException, InterruptedException {
            double soma = 0;

            for(DoubleWritable i : values){
                soma += i.get();
            }
            con.write(key, new DoubleWritable(soma));
        }
    }


    public static class MapEtapaB extends Mapper<LongWritable, Text, Text, BaseQtdWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            // pega linha
            String linha = value.toString();

            String conteudo[] = linha.split("\t");

            // pega conteudo e contagem
            String code = conteudo[0];
            String flow = conteudo[1];
            double occ = Double.parseDouble(conteudo[2]);

            // gera (chave, valor) => ("entropia", (caracter, num ocorrencias))

            con.write(new Text(flow), new BaseQtdWritable(occ, code));

        }
    }

    public static class ReduceEtapaB extends Reducer<Text, BaseQtdWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<BaseQtdWritable> values, Context con)
                throws IOException, InterruptedException {
            String code = "";
            double maior_value = 0;

            for(BaseQtdWritable b : values){
                if(b.getQtd() >= maior_value){
                    maior_value = b.getQtd();
                    code = b.getContent();
                }
            }

            con.write(new Text(key+ "\t" + code), new DoubleWritable(maior_value));

        }

    }


}
