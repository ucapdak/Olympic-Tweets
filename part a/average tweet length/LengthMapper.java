import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

public class LengthMapper extends Mapper<Object, Text, Text, IntWritable> {

    private IntWritable length = new IntWritable();
    private Text average = new Text("Average:");

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        int startIndex = 0;
        String dump = value.toString();

        if (StringUtils.ordinalIndexOf(dump,";",4)>-1)
        {
            startIndex = StringUtils.ordinalIndexOf(dump,";",3) + 1;
            String tweet = dump.substring(startIndex,dump.lastIndexOf(';'));
            length.set(tweet.length());
        }

        if (length.get()<=140)
        {
            context.write(average,length);
        }
    }
}
