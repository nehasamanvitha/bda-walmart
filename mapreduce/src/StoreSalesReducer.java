import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class StoreSalesReducer
    extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

    public void reduce(IntWritable key, Iterable<DoubleWritable> values,
                       Context context)
        throws IOException, InterruptedException {

        double totalSales = 0.0;

        for (DoubleWritable value : values) {
            totalSales += value.get();
        }

        context.write(key, new DoubleWritable(totalSales));
    }
}
