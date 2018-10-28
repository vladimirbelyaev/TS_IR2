import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SortJob extends Configured implements Tool {
    public static class SortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] keyVal = value.toString().split("\t");
            double mass = Double.parseDouble(keyVal[1].split(" ")[0]);
            context.write(new DoubleWritable(mass), new Text(keyVal[0]));
        }
    }

    public static class DoubleComparator extends WritableComparator {

        public DoubleComparator() {
            super(DoubleWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {

            Double v1 = ByteBuffer.wrap(b1, s1, l1).getDouble();
            Double v2 = ByteBuffer.wrap(b2, s2, l2).getDouble();

            return v1.compareTo(v2) * (-1);
        }
    }

    public static class SortReducer extends Reducer<DoubleWritable, Text, Text, Text> {
        @Override
        protected void reduce(DoubleWritable mass, Iterable<Text> links, Context context) throws IOException, InterruptedException {
            for (Text i: links){
                context.write(i, new Text(Double.toString(mass.get())));
            }
        }
    }

    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(SortJob.class);
        job.setJobName(SortJob.class.getCanonicalName());

        // will use traditional TextInputFormat to split line-by-line
        KeyValueTextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setSortComparatorClass(DoubleComparator.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(DoubleWritable.class);
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
        int ret = ToolRunner.run(new SortJob(), args);
        System.exit(ret);
    }
}
