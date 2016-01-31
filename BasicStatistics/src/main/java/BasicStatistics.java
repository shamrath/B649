import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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
 * Created by syodage on 1/30/16.
 */
public class BasicStatistics {

    public static class StatHelper extends Mapper<Object, Text, Text, PartialDataWritable> {

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
            PartialDataWritable data = new PartialDataWritable(
                    new DoubleWritable(partial_min),
                    new DoubleWritable(partial_max),
                    new DoubleWritable(partial_sum),
                    new DoubleWritable(partial_sqrSum),
                    new IntWritable(partial_count));
            context.write(outKey, data);
        }
    }


    public static class StatAnalyzer extends Reducer<Text, PartialDataWritable, Text, DoubleWritable> {

        double min = Double.MAX_VALUE, max = Double.MIN_VALUE, sum = 0, sqrSum = 0;
        int count = 0;
        double average = 0, std = 0;
        DecimalFormat aveFormat = new DecimalFormat("#.##");
        DecimalFormat stdFormat = new DecimalFormat("#.####");

        @Override
        protected void reduce(Text key, Iterable<PartialDataWritable> dataSet, Context context) throws IOException, InterruptedException {
            for (PartialDataWritable data : dataSet) {
                min = Math.min(data.getPartial_min().get(), min);
                max = Math.max(data.getPartial_max().get(), max);
                sum += data.getPartial_sum().get();
                sqrSum += data.getPartial_sqrsum().get();
                count += data.getPartial_cont().get();
            }
            average = sum / count;
            std = Math.sqrt((sqrSum / count) - Math.pow(average, 2));
            context.write(new Text("Min"), new DoubleWritable(min));
            context.write(new Text("Max"), new DoubleWritable(max));
            context.write(new Text("Ave"), new DoubleWritable(Double.parseDouble(aveFormat.format(average))));
            context.write(new Text("Std"), new DoubleWritable(Double.parseDouble(stdFormat.format(std))));
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Stat");
        job.setJarByClass(BasicStatistics.class);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PartialDataWritable.class);

        job.setMapperClass(StatHelper.class);
        job.setReducerClass(StatAnalyzer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

//                System.out.println("Receiving min : " + data.getPartial_min().get());
//                System.out.println("Receiving max : " + data.getPartial_max().get());
//                System.out.println("Receiving sum : " + data.getPartial_sum().get());
//                System.out.println("Receiving sqrSum : " + data.getPartial_sqrsum().get());
//                System.out.println("Receiving count : " + data.getPartial_cont().get());
