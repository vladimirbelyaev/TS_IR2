import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;

// Класс, который выполняет извлечение всех данных, которые будут нужны для train-test сетов.
public class CFJob extends Configured implements Tool {

    public static class ExtractTrainTestMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        final LongWritable one = new LongWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] val = value.toString().split("\t");
            if (val.length == 1){
                return;
            }
            String title = val[1];
            for (String word: title.split(" ")){
                context.write(new Text(word), one);
            }
            if (val.length == 3){
                String data = val[2].trim();
                for (String word: data.split(" ")){
                    context.write(new Text(word), one);
                }
            }
            else if (val.length != 2) {
                throw new InterruptedException("String has insufficient fields\n" + value.toString());
            }
        }
    }


    public static class ExtractTrainTestReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @SuppressWarnings("unchecked")
        @Override
        protected void reduce(Text word, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long result = 0;
            for (LongWritable val: values){
                result += val.get();
            }
            context.write(word, new LongWritable(result));
        }
    }

    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(CFJob.class);
        job.setJobName(CFJob.class.getCanonicalName());

        // will use traditional TextInputFormat to split line-by-line
        TextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setMapperClass(ExtractTrainTestMapper.class);
        job.setReducerClass(ExtractTrainTestReducer.class);
        job.setNumReduceTasks(10);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);


        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        //BasicConfigurator.configure();
        int ret = ToolRunner.run(new CFJob(), args);
        System.exit(ret);
    }
}
