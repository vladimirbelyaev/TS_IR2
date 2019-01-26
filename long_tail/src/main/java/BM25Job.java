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
import org.apache.hadoop.util.hash.Hash;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.*;

// Класс, который выполняет извлечение всех данных, которые будут нужны для train-test сетов.
public class BM25Job extends Configured implements Tool {
    public static class ExtractTrainTestMapper extends Mapper<LongWritable, Text, Text, Text> {
        HashMap<Long, ArrayList<Long>> doc2queries;
        HashMap<Long, HashMap<String, Double>> queries; // Имеются в виду запросы с DF, очевидно. Придется приводить к uppercase.
        @Override
        protected void setup(Context context) throws IOException, InterruptedException{
            final String DATADIR  = context.getConfiguration().get("datadir", "");
            final String QUERIES_PATH = context.getConfiguration().get("queries", "");
            if (DATADIR.equals("")){
                throw new InterruptedException("Data directory is not given");
            }
            doc2queries = Tools.getDoc2QueryMap(context, new Path(DATADIR + "all_qd_pairs"));
            queries = Tools.getQueries(context, new Path(DATADIR + QUERIES_PATH));
        }

        private String calculateTitle(String[] title, HashMap<String, Double> query){
            HashMap<String, Integer> TF = new HashMap<>();
            for (String word: title){
                if (query.containsKey(word)){
                    int curr_freq = TF.getOrDefault(word, 0) + 1;
                    TF.put(word, curr_freq);
                }
            }
            Double val = 0.0;
            for (Map.Entry<String, Integer> pair: TF.entrySet()){
                val += Math.log(query.get(pair.getKey())) * pair.getValue()/(1.0 + pair.getValue());
            }
            return val.toString();
        }

        private String calculateBody(String[] body, HashMap<String, Double> query){
            HashMap<String, Integer> TF = new HashMap<>();
            for (String word: body){
                if (query.containsKey(word)){
                    int curr_freq = TF.getOrDefault(word, 0) + 1;
                    TF.put(word, curr_freq);
                }
            }
            Double val = 0.0;
            for (Map.Entry<String, Integer> pair: TF.entrySet()){
                val += Math.log(query.get(pair.getKey())) * pair.getValue()/(1.0 + pair.getValue() + 1.0/350 * body.length);
            }
            return val.toString();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] val = value.toString().split("\t", -1);
            Long docID = Long.parseLong(val[0]);
            if (!doc2queries.containsKey(docID)){
                return;
            }
            else if (val.length == 1){
                for (Long qid: doc2queries.get(docID)){
                    context.write(new Text(qid.toString() + "\t" + docID.toString()), new Text("0.0\t0.0"));
                }
            }
            else if (val.length == 2){ // Только title
                String[] title = val[1].split(" ");
                for (Long qid: doc2queries.get(docID)){
                    HashMap<String, Double> query = queries.get(qid);
                    String titleScore = calculateTitle(title, query);
                    context.write(new Text(qid.toString() + "\t" + docID.toString()), new Text(titleScore + "\t0.0"));
                }
            }
            else if (val.length == 3){
                for (Long qid: doc2queries.get(docID)){
                    String titleScore;
                    String bodyScore;
                    if (!val[1].equals("")){
                        String[] title = val[1].split(" ");
                        titleScore = calculateTitle(title, queries.get(qid));
                    }else{
                        titleScore = "0.0";
                    }
                    if (!val[2].equals("")){
                        String[] body = val[1].split(" ");
                        bodyScore = calculateBody(body, queries.get(qid));
                    }else{
                        bodyScore = "0.0";
                    }
                    context.write(new Text(qid.toString() + "\t" + docID.toString()), new Text(titleScore + "\t" + bodyScore));
                }
            }
            else {
                throw new InterruptedException("Line has more fields than expected");
            }
        }
    }


    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(BM25Job.class);
        job.setJobName(BM25Job.class.getCanonicalName());

        // will use traditional TextInputFormat to split line-by-line
        TextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setMapperClass(ExtractTrainTestMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        //BasicConfigurator.configure();
        int ret = ToolRunner.run(new BM25Job(), args);
        System.exit(ret);
    }
}
