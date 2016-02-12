package indiana.cgl.hadoop.pagerank.helper;

import java.io.IOException;
 
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.Reducer;

public class CleanupResultsReduce extends Reducer<LongWritable, Text, LongWritable, Text> {

    private static int count = 0;
    private static double prSum = 0.0;
    public void reduce(LongWritable key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {
        int numUrls = context.getConfiguration().getInt("numUrls", 1);
        Text value = values.iterator().next();
        prSum += Double.valueOf(value.toString());
        count++;
        context.write(key, values.iterator().next());
        if (count == numUrls) {
            System.out.println("Page Ranks sum :" + prSum);
        }
    }
}