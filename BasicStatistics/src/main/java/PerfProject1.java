import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * Created by syodage on 1/29/16.
 */
public class PerfProject1 {


    public static class StatHelper extends Mapper<Object, Text, Text, TupleWritable> {

        private Text outKey = new Text("reduce");
        double partial_min = Double.MAX_VALUE, partial_max = Double.MIN_VALUE, partial_sum = 0, partial_sqrSum = 0;
        int partial_count = 0;
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            double val = Double.parseDouble(value.toString());
            partial_min = Math.min(val, partial_min);
            partial_max = Math.max(val, partial_max);
            partial_sum += val;
            partial_sqrSum += Math.pow(val, 2);
            partial_count++;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Writable[] values = new Writable[5];
            values[0] = new DoubleWritable(partial_min);
            values[1] = new DoubleWritable(partial_max);
            values[2] = new DoubleWritable(partial_sum);
            values[3] = new DoubleWritable(partial_sqrSum);
            values[4] = new IntWritable(partial_count);
            System.out.println("Sending partial_min : " + ((DoubleWritable) values[0]).get());
            System.out.println("Sending partial_max : " + ((DoubleWritable) values[1]).get());
            System.out.println("Sending partial_sum : " + ((DoubleWritable) values[2]).get());
            System.out.println("Sending partial_sqrSum : " + ((DoubleWritable) values[3]).get());
            System.out.println("Sending partial_count : " + ((IntWritable) values[4]).get());
            context.write(outKey, new TupleWritable(values));
        }
    }


    public static class StatAnalyzer extends Reducer<Text, TupleWritable, Text, DoubleWritable> {

        double min = Double.MAX_VALUE, max = Double.MIN_VALUE, sum = 0, sqrSum = 0;
        int count = 0;
        double average = 0, std = 0;
        DecimalFormat aveFormat = new DecimalFormat("#.##");
        DecimalFormat stdFormat = new DecimalFormat("#.####");
        @Override
        protected void reduce(Text key, Iterable<TupleWritable> tuples, Context context) throws IOException, InterruptedException {
            for (TupleWritable tuple : tuples) {
                min = Math.min(((DoubleWritable) tuple.get(0)).get(), min);
                max = Math.max(((DoubleWritable) tuple.get(1)).get(), max);
                sqrSum += Math.pow(((DoubleWritable) tuple.get(3)).get(), 2);
                sum += ((DoubleWritable) tuple.get(2)).get();
                count += ((IntWritable) tuple.get(4)).get();
                System.out.println("Receiving min : " + ((DoubleWritable) tuple.get(0)).get());
                System.out.println("Receiving max : " + ((DoubleWritable) tuple.get(1)).get());
                System.out.println("Receiving sum : " + ((DoubleWritable) tuple.get(2)).get());
                System.out.println("Receiving sqrSum : " + ((DoubleWritable) tuple.get(3)).get());
                System.out.println("Receiving count : " + ((IntWritable) tuple.get(4)).get());
            }

            average = sum / count;
            std = Math.sqrt((sqrSum / count) - Math.pow(average, 2));
            context.write(new Text("pMin --> "), new DoubleWritable(min));
            context.write(new Text("pMax --> "), new DoubleWritable(max));
//            context.write(new Text("pAve --> "), new DoubleWritable(Double.parseDouble(aveFormat.format(average))));
//            context.write(new Text("pStd --> "), new DoubleWritable(Double.parseDouble(stdFormat.format(std))));
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Stat");
        job.setJarByClass(PerfProject1.class);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TupleWritable.class);

        job.setMapperClass(StatHelper.class);
        job.setReducerClass(StatAnalyzer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
