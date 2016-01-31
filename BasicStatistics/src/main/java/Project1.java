import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * Created by syodage on 1/29/16.
 */
public class Project1 {

    public static class TypeConvertor extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text word = new Text("reduce"); // send to same reducer.
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(word, new DoubleWritable(Double.parseDouble(value.toString()))); // convert Text to DoubleWriter
        }
    }

    public static class StatAnalyzer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        double average =  0.0;
        double std = 0.0;
        int count = 0;
        double sum = 0;
        double sqrSum = 0;
        DecimalFormat aveFormat = new DecimalFormat("#.##");
        DecimalFormat stdFormat = new DecimalFormat("#.####");
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            for (DoubleWritable value : values) {
                min = Math.min(value.get(), min);
                max = Math.max(value.get(), max);
                sum += value.get();
                sqrSum += Math.pow(value.get(), 2);
                count++;

            }
            average = sum / count;
            std = Math.sqrt((sqrSum / count) - Math.pow(average, 2));
            context.write(new Text("Min --> "), new DoubleWritable(min));
            context.write(new Text("Max --> "), new DoubleWritable(max));
            context.write(new Text("Ave --> "), new DoubleWritable(Double.parseDouble(aveFormat.format(average))));
            context.write(new Text("Std --> "), new DoubleWritable(Double.parseDouble(stdFormat.format(std))));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        for (String arg : args) {
            System.out.println(arg);
        }
        Job job = Job.getInstance(conf, "Min");
        job.setJarByClass(Project1.class);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(new Path(args[1]))){
            fs.delete(new Path(args[1]),true);
        }
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(TypeConvertor.class);
        job.setReducerClass(StatAnalyzer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
