package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;
import org.apache.log4j.BasicConfigurator;
import java.io.IOException;

public class ForestFire {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "forestfire");

        // definicao de classes
        j.setJarByClass(ForestFire.class);
        j.setMapperClass(ForestFireMapper.class);
        j.setReducerClass(ForestFireReducer.class);
        j.setCombinerClass(ForestFireReducer.class);

        // definicao de tipos
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(ForestFireWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(ForestFireWritable.class);

        // definicao de arquivos
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class ForestFireMapper extends Mapper<Object, Text, Text, ForestFireWritable> {
        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {
            // obtendo linha para processamento
            String linha = value.toString();

            // quebrando em campos
            String campos[] = linha.split(",");

            // obtendo mes, vento e temperatura
            String mes = campos[2];
            double temp = Double.parseDouble(campos[8]);
            double wind = Double.parseDouble(campos[10]);

            // mandando chave = mes, valor = (temp, wind)
            context.write(new Text(mes), new ForestFireWritable(temp, wind));
        }
    }

    public static class ForestFireReducer extends Reducer<Text, ForestFireWritable, Text, ForestFireWritable> {

        public void reduce(Text key,
                           Iterable<ForestFireWritable> values,
                           Context context) throws IOException, InterruptedException {
            // para cada chave (mes), encontrar maior valor de temperatura e vento
            // importante: esta funcao serve tanto para reduce quanto para combiner!

            double maxTemp = Double.MIN_VALUE;
            double maxWind = Double.MIN_VALUE;
            for(ForestFireWritable o : values){
                if (o.getTemp() > maxTemp) maxTemp = o.getTemp();
                if (o.getWind() > maxWind) maxWind = o.getWind();
            }

            // enviando os maiores valores encontrados
            context.write(key, new ForestFireWritable(maxTemp, maxWind));

        }
    }
}
