import clickmodels.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

// Класс, который выполняет извлечение всех данных, которые будут нужны для train-test сетов.
public class ExtractAllJob extends Configured implements Tool {

    public static class ExtractTrainTestMapper extends Mapper<LongWritable, Text, Text, Text> {
        // На самом деле, нам интересно менять только урлы. Даже не менять, а проверять на наличие. Остальное потом.
        HashSet<String> urlSet;
        HashSet<String> querySet;
        HashSet<String> specialSet;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException, NullPointerException{
            final String DATADIR  = context.getConfiguration().get("datadir", "");
            if (DATADIR.equals("")){
                throw new InterruptedException("Data directory is not given");
            }
            final String URLSETPATH = DATADIR + "urlset";
            final String QUERYSETPATH = DATADIR + "queryset";
            urlSet = Tools.getHashSet(context, new Path(URLSETPATH));
            querySet = Tools.getHashSet(context, new Path(QUERYSETPATH));
            specialSet = Tools.getSpecialParts();
        }
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            SERPLog log = new SERPLog(value.toString(), urlSet, specialSet);
            if (querySet.contains(log.query)) {
                FastSDBNMapper sdbn = new FastSDBNMapper();
                sdbn.map(context, log);
                CTRMapper ctr = new CTRMapper();
                ctr.map(context, log);
                ShowPosMapper shp = new ShowPosMapper();
                shp.map(context, log);
                QueryCTRMapper qctr = new QueryCTRMapper();
                qctr.map(context, log);
            }
            MeanCTRMapper mctr = new MeanCTRMapper();
            mctr.map(context, log);
            MeanShowPosMapper mshp = new MeanShowPosMapper();
            mshp.map(context, log);
            RankCTRPosMapper rctr = new RankCTRPosMapper();
            rctr.map(context, log);
        }
    }


    public static class ExtractTrainTestReducer extends Reducer<Text, Text, Text, Text> {
        HashMap<String, Long> trainMarks;
        HashMap<String, String> sampleNames;
        HashMap<String, Long> urlMap;
        private final String DELIMETER = "::::";
        private MultipleOutputs<Text, Text> out;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException, NullPointerException{
            out = new MultipleOutputs<>(context);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected void reduce(Text queryurltxt, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] metricPair = queryurltxt.toString().split(DELIMETER);
            boolean error = true;
            if (metricPair[0].equals(CTRReducer.MARKER)){
                String query = metricPair[1];
                String url = metricPair[2];
                error = false;
                CTRReducer.reduce(query, url, values, out);
            }
            if (metricPair[0].equals(FastSDBNReducer.MARKER)){
                String query = metricPair[1];
                String url = metricPair[2];
                error = false;
                FastSDBNReducer.reduce(query, url, values, out);
            }
            if (metricPair[0].equals(ShowPosReducer.MARKER)){
                String query = metricPair[1];
                String url = metricPair[2];
                error = false;
                ShowPosReducer.reduce(query, url, values, out);
            }
            if (metricPair[0].equals(MeanCTRReducer.MARKER)){
                String url = metricPair[1];
                error = false;
                MeanCTRReducer.reduce(url, values, out);
            }
            if (metricPair[0].equals(MeanShowPosReducer.MARKER)){
                String url = metricPair[1];
                error = false;
                MeanShowPosReducer.reduce(url, values, out);
            }
            if (metricPair[0].equals(RankCTRPosReducer.MARKER)){
                String url = metricPair[1];
                error = false;
                RankCTRPosReducer.reduce(url, values, out);
            }
            if (metricPair[0].equals(QueryCTRReducer.MARKER)){
                String query = metricPair[1];
                error = false;
                QueryCTRReducer.reduce(query, values, out);
            }
            if (error){
                throw new InterruptedException("No appropriate reducer found");
            }
        }

        @Override
        protected void cleanup(Context context
        ) throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            super.cleanup(context);
            out.close();
        }
    }

    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(ExtractAllJob.class);
        job.setJobName(ExtractAllJob.class.getCanonicalName());

        // will use traditional TextInputFormat to split line-by-line
        TextInputFormat.addInputPath(job, new Path(input));

        //CombineFileInputFormat.addInputPath(job, new Path(input));
        //job.setInputFormatClass(CombineFileInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output));
        MultipleOutputs.addNamedOutput(job, "xgb", TextOutputFormat.class,
                Text.class, Text.class);

        job.setMapperClass(ExtractTrainTestMapper.class);
        job.setReducerClass(ExtractTrainTestReducer.class);
        job.setNumReduceTasks(10);
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
        //BasicConfigurator.configure();
        int ret = ToolRunner.run(new ExtractAllJob(), args);
        System.exit(ret);
    }
}
