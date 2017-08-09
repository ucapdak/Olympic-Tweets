import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LengthReducer extends Reducer<Text, IntWritable, Text, DoubleWritable>
{
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
        int sum = 0;
        int size = 0;
        double average = 0;

        for (IntWritable value : values)
        {
            sum = sum + value.get();
            size++;
        }

        average = (double) sum / (double) size;

        result.set(average);
        context.write(key,result);
    }
}
