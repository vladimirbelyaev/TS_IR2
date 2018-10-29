import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class TestingPageRankJob extends Configured implements Tool {
    public static class TestPRMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        static final Text _key = new Text("all");
        static final Text _leak = new Text("potential_leak");
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String data = value.toString().split("\t")[1];
            PageRankNode node = PageRankNode.read(data, false);
            DoubleWritable w = new DoubleWritable(node.weightOut);
            context.write(_key, w);
            if (node.linksOut.size() == 0){
                context.write(_leak, w);
            }
        }
    }

    public static class TestPRReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        @Override
        protected void reduce(Text word, Iterable<DoubleWritable> nums, Context context) throws IOException, InterruptedException {
            double sum = 0;
            for(DoubleWritable i: nums) {
                sum += i.get();
            }

            // produce pairs of "word" <-> amount
            context.write(word, new Text(Double.toString(sum)));
        }
    }

    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(TestingPageRankJob.class);
        job.setJobName(TestingPageRankJob.class.getCanonicalName());

        // will use traditional TextInputFormat to split line-by-line
        KeyValueTextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(TestPRMapper.class);
        job.setReducerClass(TestPRReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
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
        int ret = ToolRunner.run(new TestingPageRankJob(), args);
        System.exit(ret);
    }
}