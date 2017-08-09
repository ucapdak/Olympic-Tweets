import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

public class DateMapper extends Mapper<Object, Text, Text, IntWritable> {

    private IntWritable one = new IntWritable(1);
    private Text date = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        int startIndex = 0;
        String dump = value.toString();
        String dateString = "";

        startIndex = StringUtils.ordinalIndexOf(dump,";",1) + 1;
        dateString = dump.substring(startIndex,startIndex+10);

        date.set(dateString);
        context.write(date,one);
    }
}