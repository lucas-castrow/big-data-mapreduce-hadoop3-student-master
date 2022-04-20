package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;
import org.apache.log4j.BasicConfigurator;
import java.io.IOException;

public class MaxMinMeanPrice {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "maxminmean");

        // Registro de classes
        j.setJarByClass(MaxMinMeanPrice.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(ReduceForAverage.class);
//        j.setCombinerClass(CombineForAverage.class);

        // Definição de tipos de saida
        // map (Text, FireAvgTempWritable)
        j.setMapOutputKeyClass(UnitTypeAndYear.class);
        j.setMapOutputValueClass(DoubleWritable.class);
        // reduce (Text, DoubleWritable)
        j.setOutputKeyClass(UnitTypeAndYear.class);
        j.setOutputValueClass(Text.class);

        // definicao de arquivos
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForAverage extends Mapper<LongWritable, Text, UnitTypeAndYear, DoubleWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            // obtendo linha para parocessamento
            String linha = value.toString();

            // quebrando em campos
            String campos[] = linha.split(";");

            // obtendo mes, vento e temperatura


            if(!campos[1].equals("year") && (!campos[1].isEmpty()) && (!campos[6].isEmpty()) && !(Double.parseDouble(campos[6]) == 0.0) &&  (!campos[7].isEmpty())) {
                // mandando chav&&  = mes, valor = (temp, wind)
                double price = Double.parseDouble(campos[6]);
                String year = campos[1];
                String unit = campos[7];
                con.write(new UnitTypeAndYear(unit, year), new DoubleWritable(price));
            }
        }
    }

    public static class ReduceForAverage extends Reducer<UnitTypeAndYear, DoubleWritable,
            UnitTypeAndYear, Text> {
        public void reduce(UnitTypeAndYear key, Iterable<DoubleWritable> values, Context con)
                throws IOException, InterruptedException {

            // para cada chave (mes), encontrar maior valor de temperatura e vento
            // importante: esta funcao serve tanto para reduce quanto para combiner!

            double maxPrice = Double.MIN_VALUE;
            double minPrice = Double.MAX_VALUE;
            int occ = 0;
            double sum = 0;
            for(DoubleWritable o : values){
                occ += 1;
                sum += o.get();
                if (o.get() > maxPrice) maxPrice = o.get();
                if (o.get() < minPrice) minPrice = o.get();
            }

            double media = sum / occ;

            String result = String.valueOf(maxPrice) + "    " + String.valueOf(minPrice) + "    "+ String.valueOf(media);
            // enviando os maiores valores encontrados
            con.write(key, new Text(result));
        }
    }
}
