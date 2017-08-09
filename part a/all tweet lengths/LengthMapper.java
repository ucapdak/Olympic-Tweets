import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

public class LengthMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final IntWritable one = new IntWritable(1);
    private Text lengthRange = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        int startIndex = 0;
        String dump = value.toString();
        int closestFive = 0;
        String range = "";

        if (StringUtils.ordinalIndexOf(dump,";",4)>-1)
        {
            startIndex = StringUtils.ordinalIndexOf(dump,";",3) + 1;
            String tweet = dump.substring(startIndex,dump.lastIndexOf(';'));
            closestFive = roundToFive(tweet.length());
            range = ""+numberToText(closestFive-4)+" - "+numberToText(closestFive)+":";
            lengthRange.set(range);
        }

        context.write(lengthRange,one);
    }
    public int roundToFive(int n)
    {
        return ((n + 4) / 5) * 5;
    }
    public String numberToText(int n)
    {
        if (n < 10)
        {
            return "00"+n;
        }
        else if (n < 100)
        {
            return "0"+n;
        }
        else
        {
            return ""+n;
        }
    }
}
