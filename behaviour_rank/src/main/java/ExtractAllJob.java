import SERPLog.SERPLogUnhashed;
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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

// Класс, который выполняет извлечение всех данных, которые будут нужны для train-test сетов.
public class ExtractAllJob extends Configured implements Tool {

    public static class ExtractTrainTestMapper extends Mapper<LongWritable, Text, Text, Text> {

        HashMap<String, Long> queryMap;
        HashSet<String> trainConvertedSet;
        HashSet<String> sampleConvertedSet;
        HashMap<String, Long> urlMap;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException, NullPointerException{
            final String DATADIR  = context.getConfiguration().get("datadir", "");
            if (DATADIR.equals("")){
                throw new InterruptedException("Data directory is not given");
            }
            // Запрос->ключ. Для проверки наличия запроса в базе.
            queryMap = Tools.getMapKey1(context, new Path(DATADIR+ "/queries"), false, "\t");
            urlMap = Tools.getMapKey1(context, new Path(DATADIR + "urldata"), false, "\t");

            // Для ускоренного поиска внутри BSDN
            String TRAINCONVERTEDPATH = DATADIR + "train_converted_no_marks";
            String SAMPLECONVERTEDPATH = DATADIR + "sample_converted";

            trainConvertedSet = Tools.getHashSet(context, new Path(TRAINCONVERTEDPATH));
            sampleConvertedSet = Tools.getHashSet(context, new Path(SAMPLECONVERTEDPATH));


        }
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            SERPLogUnhashed log = new SERPLogUnhashed(value.toString());
            if (queryMap.containsKey(log.query)) {
                FastSDBNMapper sdbn = new FastSDBNMapper(trainConvertedSet, sampleConvertedSet);
                sdbn.map(context, log);
                CTRMapper ctr = new CTRMapper(trainConvertedSet, sampleConvertedSet);
                ctr.map(context, log);
            }

            MeanCTRMapper mctr = new MeanCTRMapper(urlMap);
            mctr.map(context, log);
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
            final String DATADIR  = context.getConfiguration().get("datadir", "");
            if (DATADIR.equals("")){
                throw new InterruptedException("Data directory is not given");
            }
            String TRAINCONVERTEDPATH = DATADIR + "train_converted";
            trainMarks = Tools.getMapKey0(context, new Path(TRAINCONVERTEDPATH), false, "\t");
            String SAMPLENAMESPATH = DATADIR + "samplemap";
            sampleNames = Tools.getSMap(context, new Path(SAMPLENAMESPATH), false, "\t");
            urlMap = Tools.getMapKey1(context, new Path(DATADIR + "urldata"), false, "\t");
            System.out.println("End setup");
        }

        @SuppressWarnings("unchecked")
        @Override
        protected void reduce(Text queryurltxt, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] metricPair = queryurltxt.toString().split(DELIMETER);
            String queryurl = metricPair[1];
            boolean error = true;
            if (metricPair[0].equals(CTRReducer.MARKER)){
                error = false;
                CTRReducer.reduce(queryurl, values, trainMarks, sampleNames, out);
            }
            if (metricPair[0].equals(FastSDBNReducer.MARKER)){
                error = false;
                FastSDBNReducer.reduce(queryurl, values, trainMarks, sampleNames, out);
            }
            if (metricPair[0].equals(MeanCTRReducer.MARKER)){
                error = false;
                MeanCTRReducer.reduce(queryurl, values, urlMap, out);
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
