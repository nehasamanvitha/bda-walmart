import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class StoreSalesMapper
    extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        String line = value.toString();

        if (line.startsWith("Store,")) {
            return;
        }

        String[] fields = line.split(",");

        if (fields.length == 5) {
            try {
                int store = Integer.parseInt(fields[0].trim());
                double sales = Double.parseDouble(fields[3].trim());

                context.write(
                    new IntWritable(store),
                    new DoubleWritable(sales)
                );
            } catch (NumberFormatException e) {
                // Skip invalid records
            }
        }
    }
}
