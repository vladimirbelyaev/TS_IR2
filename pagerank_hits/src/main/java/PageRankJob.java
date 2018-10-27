import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.util.ArrayList;

/* Логика работы:
Файлы с весами отдельно, структура отдельно.
Число сайтов берем из файла.
Первое заполнение весов надо отдельно закодить(можно этого и не делать, получим PageRank * n_links.
Random jump: знаем N, пересчитываем в Reducer.
Веса висячих вершин: кидаем в отдельные файлы, потом суммируем, в Mapper'е делаем добавку.
 */
public class PageRankJob extends Configured implements Tool {
    public static class PageRankMapper extends Mapper<Text, Text, Text, Text> {
        double alpha = 0.01;
        long N;
        final Text hangingLink = new Text("HANGING_LINK");
        double avgLeak = 0;

        public long getN(JobContext context)throws IOException, InterruptedException{
            Path urls = new Path("hw_pagerank/unique/part-r-00000");
            FileSystem fs = urls.getFileSystem(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(urls)));
            String curLine = br.readLine();
            long N = Long.parseLong(curLine.split("\t")[1]);
            br.close();
            return N;
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException{
            N = getN(context);
            String[] dirName = ((FileSplit) context.getInputSplit()).getPath().getParent().getName().split("/");
            String fileName =  "/user/v.belyaev/hw_pagerank/" + dirName[dirName.length - 1] + "/leak-r-00000";
            Path leakFile = new Path(fileName);
            FileSystem fs = leakFile.getFileSystem(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(leakFile)));
            String curLine = br.readLine().trim();
            double leakedPR =  Double.parseDouble(curLine.split("\t")[1]);
            avgLeak = leakedPR/N;
        }
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
            PageRankNode nodeIn = PageRankNode.read(value.toString());
            PageRankNode nodeOut = new PageRankNode();
            nodeIn.weight = nodeIn.weight + avgLeak;
            System.out.println(key.toString() + "\t" + value.toString());
            if (nodeIn.linksOut.size() == 0){
                System.out.println(hangingLink.toString() + " " + key.toString());
                context.write(hangingLink, nodeIn.toText());
            }
            else{
                nodeOut.weight = nodeIn.weight/nodeIn.linksOut.size();
                for (String i: nodeIn.linksOut){
                    context.write(new Text(i), nodeOut.toText());
                }
            }
        }
    }
    public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
        double alpha = 0.01;
        long N;
        final Text hangingLink = new Text("HANGING_LINK");
        private MultipleOutputs<Text, Text> out;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException{
            // Ставим multiple output
            out = new MultipleOutputs<>(context);

            // Здесь надо прописать N
            Path urls = new Path("hw_pagerank/unique/part-r-00000");
            FileSystem fs = urls.getFileSystem(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(urls)));
            String curLine = br.readLine();
            N = Long.parseLong(curLine.split("\t")[1]);
            br.close();
        }
        @Override
        protected void reduce(Text key, Iterable<Text> data, Context context) throws IOException, InterruptedException {
            if (key.toString().equals(hangingLink.toString())){
                System.out.println(key.toString());
                // Записать отдельно
                PageRankNode node = new PageRankNode();
                for (Text i: data){
                    node.weight += PageRankNode.read(i.toString()).weight;
                }
                out.write("leak", key, node.toText(), "leak");
            }
            else{
                PageRankNode node = new PageRankNode();
                for (Text i: data){
                    PageRankNode curNode = PageRankNode.read(i.toString());
                    if (curNode.linksOut.size() == 0){
                        node.weight += curNode.weight;
                    }
                    else{
                        node.linksOut = curNode.linksOut;
                    }
                }
                node.weight = node.weight * (1 - alpha) + alpha * 1.0 / N;
                context.write(key, node.toText());
            }

        }
    }

    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(PageRankJob.class);
        job.setJobName(PageRankJob.class.getCanonicalName());

        KeyValueTextInputFormat.addInputPath(job, new Path(input));
        MultipleOutputs.addNamedOutput(job, "leak", TextOutputFormat.class,
                Text.class, Text.class);
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);
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
        String dirName = args[0]; // hw_pagerank/iter_
        int nIter = Integer.parseInt(args[1]);
        String[] modifiedArgs = new String[2];
        int ret = 0;
        for (int i = 0; i < nIter - 1; i++){
            modifiedArgs[0] = dirName + Integer.toString(i) + "/part-*";
            modifiedArgs[1] = dirName + Integer.toString(i + 1);
            System.out.println("Starting iteration " + Integer.toString(i));
            ToolRunner.run(new PageRankJob(), modifiedArgs);
        }
        modifiedArgs[0] = dirName + Integer.toString(nIter - 1) + "/part-*";
        modifiedArgs[1] = dirName + "fin";
        ret = ToolRunner.run(new PageRankJob(), modifiedArgs);
        System.exit(ret);
    }
}
