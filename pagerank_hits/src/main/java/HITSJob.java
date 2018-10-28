import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class HITSJob extends Configured implements Tool {
    public static class HITSMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            PageRankNode node = PageRankNode.readHITS(value.toString());
            context.write(key, new Text("s" + value.toString())); // Бросаем внутреннюю структуру
            for (String linkOut:node.linksOut){ // Страницы, на которые переходим
                context.write(new Text(linkOut), new Text("h" + Double.toString(node.weightOut)));
            }
            for (String linkIn:node.linksIn){ // Страницы, с которых переходят
                //weightIn
                context.write(new Text(linkIn), new Text("a" + Double.toString(node.weightIn)));
            }
        }
    }

    public static class HITSCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text word, Iterable<Text> data, Context context) throws IOException, InterruptedException {
            double weightIn = 0;
            double weightOut = 0;
            System.out.println("Combining " + word.toString());
            for(Text i: data) {
                String info = i.toString();
                String token = info.substring(0,1);
                if (token.equals("s")){
                    context.write(word, i);
                }
                else if (token.equals("h")){
                    weightIn += Double.parseDouble(info.substring(1));
                }
                else if (token.equals("a")){
                    weightOut += Double.parseDouble(info.substring(1));
                }
            }
            context.write(word, new Text("h" + Double.toString(weightIn)));
            context.write(word, new Text("a" + Double.toString(weightOut)));
        }
    }


    public static class HITSReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text word, Iterable<Text> data, Context context) throws IOException, InterruptedException {
            System.out.println(word.toString());
            PageRankNode node = new PageRankNode();
            long counter = 0;
            boolean gotGraph = false;
            for(Text i: data) {
                counter += 1;
                if (counter % 10000 == 0){
                    System.out.println(counter);
                }
                String info = i.toString();
                String token = info.substring(0,1);
                if (token.equals("s")){
                    System.out.println("Parsing graph");
                    PageRankNode graphNode = PageRankNode.readHITS(info.substring(1));
                    System.out.println("Graph parsed");
                    node.linksOut = graphNode.linksOut;
                    node.linksIn = graphNode.linksIn;
                    gotGraph = true;
                }
                else if (token.equals("h")){
                    node.weightIn += Double.parseDouble(info.substring(1));
                }
                else if (token.equals("a")){
                    node.weightOut += Double.parseDouble(info.substring(1));
                }
            }
            System.out.println("Writing node");
            context.write(word, node.toHITS());
            System.out.println("Node written");
        }
    }

    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(HITSJob.class);
        job.setJobName(HITSJob.class.getCanonicalName());

        // will use traditional TextInputFormat to split line-by-line
        KeyValueTextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setNumReduceTasks(5);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapperClass(HITSMapper.class);
        job.setReducerClass(HITSReducer.class);
        job.setCombinerClass(HITSCombiner.class);
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
        int ret = ToolRunner.run(new HITSJob(), args);
        System.exit(ret);
    }
}
