import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
/* Логика работы:
Файлы с весами отдельно, структура отдельно.
Число сайтов берем из файла.
Первое заполнение весов надо отдельно закодить(можно этого и не делать, получим PageRank * n_links.
Random jump: знаем N, пересчитываем в Reducer.
Веса висячих вершин: кидаем в отдельные файлы, потом суммируем, в Mapper'е делаем добавку.
 */
public class InitPageRankJob extends Configured implements Tool {

    public static class InitPageRankMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }
    public static class InitPageRankReducer extends Reducer<Text, Text, Text, Text> {
        public long getN(Reducer.Context context)throws IOException, InterruptedException {
            Path unique_file = new Path("hw_pagerank/unique/part-r-00000");
            FileSystem fs = unique_file.getFileSystem(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(unique_file)));
            String line = br.readLine();
            long N = Long.parseLong(line.split("\t")[1]);
            br.close();
            return N;
        }
        String mass;
        private MultipleOutputs<Text, Text> out;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException{
            // Здесь надо прописать N
            mass = Double.toString(1.0/getN(context));
            out = new MultipleOutputs<>(context);

        }
        @Override
        protected void reduce(Text key, Iterable<Text> text, Context context) throws IOException, InterruptedException {
            int counter = 0;
            for (Text i:text){
                counter += 1;
                if (!i.toString().equals("")) {
                    context.write(key, new Text(mass + " " + i.toString()));
                }
                else{
                    context.write(key, new Text(mass));
                }
            }
            assert (counter == 1);
            out.write("leak", new Text("HANGING_LINK"), new Text("0.0"), "leak");
        }
    }

    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(InitPageRankJob.class);
        job.setJobName(InitPageRankJob.class.getCanonicalName());

        KeyValueTextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setMapperClass(InitPageRankMapper.class);
        job.setReducerClass(InitPageRankReducer.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        MultipleOutputs.addNamedOutput(job, "leak", TextOutputFormat.class,
                Text.class, Text.class);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new InitPageRankJob(), args);
        System.exit(ret);
    }
}

