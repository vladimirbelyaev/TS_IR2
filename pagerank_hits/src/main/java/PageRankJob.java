import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
// На вход - id+base64(gzip(html))
// На выход - id->ids from urls.txt
public class PageRankJob extends Configured implements Tool {
    public static class LinkMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        }
    }
    public static class LinkReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> text, Context context) throws IOException, InterruptedException {
            String links = "";
            for (Text i:text){
                links += i.toString() + " ";
            }
            context.write(key, new Text(links.trim()));
        }
    }

    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(GetLinksJob.class);
        job.setJobName(GetLinksJob.class.getCanonicalName());
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(LinkMapper.class);
        job.setReducerClass(LinkReducer.class);

        KeyValueTextInputFormat.addInputPath(job, new Path(input));
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new GetLinksJob(), args);
        System.exit(ret);
    }
}
