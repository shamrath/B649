import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by syodage on 1/30/16.
 */
public class PartialDataWritable implements Writable, Serializable {

    DoubleWritable partial_min = new DoubleWritable();
    DoubleWritable partial_max = new DoubleWritable();
    DoubleWritable partial_sum = new DoubleWritable();
    DoubleWritable partial_sqrsum = new DoubleWritable();
    IntWritable partial_cont = new IntWritable();

    public PartialDataWritable() {
    }

    public PartialDataWritable(DoubleWritable partial_min, DoubleWritable partial_max, DoubleWritable partial_sum, DoubleWritable partial_sqrsum, IntWritable partial_cont) {
        this.partial_min = partial_min;
        this.partial_max = partial_max;
        this.partial_sum = partial_sum;
        this.partial_sqrsum = partial_sqrsum;
        this.partial_cont = partial_cont;
    }

    public void write(DataOutput out) throws IOException {
        partial_min.write(out);
        partial_max.write(out);
        partial_sum.write(out);
        partial_sqrsum.write(out);
        partial_cont.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        partial_min.readFields(in);
        partial_max.readFields(in);
        partial_sum.readFields(in);
        partial_sqrsum.readFields(in);
        partial_cont.readFields(in);
    }


    public DoubleWritable getPartial_min() {
        return partial_min;
    }

    public DoubleWritable getPartial_max() {
        return partial_max;
    }

    public DoubleWritable getPartial_sum() {
        return partial_sum;
    }

    public DoubleWritable getPartial_sqrsum() {
        return partial_sqrsum;
    }

    public IntWritable getPartial_cont() {
        return partial_cont;
    }
}
