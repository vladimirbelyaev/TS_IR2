import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Waitable;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

// На вход - id+base64(gzip(html))
// На выход - id->ids from urls.txt
public class GetLinksJob extends Configured implements Tool {
    public static class NodeWritable implements Writable{
        double weight;
        ArrayList<String> linksOut = new ArrayList<>();
        @Override
        public void readFields(DataInput in) throws IOException{
            String[] data = in.toString().split(" ");
            weight = Double.parseDouble(data[0]);
            for (int i = 1; i < data.length; i++){
                linksOut.add(data[i]);
            }
        }

        @Override
        public void write(DataOutput out) throws IOException{
            out.writeDouble(weight);
            for (String link : linksOut) {
                out.writeChars(" ");
                out.writeChars(link);
            }
        }
    }

    public static class LinkMapper extends Mapper<Text, Text, Text, Text> {
        HashMap<String, String> idToPage;
        HashMap<String, String> pageToId;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException{
            idToPage = new HashMap<>();
            pageToId = new HashMap<>();
            Path urls = new Path("/data/infopoisk/hits_pagerank/urls.txt");
            FileSystem fs = urls.getFileSystem(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(urls)));
            String curLine;
            String[] keyVal;
            while ((curLine = br.readLine()) != null){
                keyVal = curLine.split("\t");
                idToPage.put(keyVal[0],keyVal[1]);
                pageToId.put(keyVal[1],keyVal[0]);
            }
            br.close();
        }

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            //System.out.println(key.toString());
            //System.out.println(value.toString());
            String line = value.toString().replace("\n", "");
            Base64.Decoder decoder = Base64.getDecoder();
            byte[] decodedByteArray = decoder.decode(line);


            Inflater inflater = new Inflater();
            inflater.setInput(decodedByteArray);

            byte[] page_buf = new byte[4096];
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            for (int len = 0; !inflater.finished(); bos.write(page_buf, 0, len), len = 0) {
                try {
                    len = inflater.inflate(page_buf, 0, page_buf.length);
                } catch (DataFormatException e) {
                    throw new IOException("Inflater ERROR: " + e.getMessage());
                }
            }
            bos.close();
            String regex = "\\<a\\s.*?href=(?:\\\"([\\w\\.:/?=&#%_\\-]*)\\\"|([^\\\"][\\w\\.:/?=&#%_\\-]*[^\\\"\\>])).*?\\>";
            Pattern parse_link = Pattern.compile(regex);
            Matcher match = parse_link.matcher(bos.toString(StandardCharsets.UTF_8.name()));
            //LongWritable key_num = new LongWritable(Long.parseLong(lines[0]));
            //LongWritable key_num = new LongWritable(Long.parseLong(key.toString()));
            Text key_num = new Text(idToPage.get(key.toString()));
            while(match.find()){
                String subSplit = match.group(0).split("href=")[1];
                String[] finalSplit = subSplit.split("\"");
                if (finalSplit.length > 1){
                    String candidate = finalSplit[1];
                    if (!candidate.contains("http")){
                        candidate = "http://lenta.ru" + candidate;
                    }
                    context.write(key_num, new Text(candidate.replace("www.","")));
                }
                else{
                    String candidate = subSplit.trim();
                    if (candidate.contains("http")){
                        Text result = new Text(candidate.split(" ")[0].replace(">","").replace("www.",""));
                        context.write(key_num, result);
                    }
                    else if (!candidate.contains("http") & candidate.contains("www.")){
                        candidate = "http://" + candidate;
                        Text result = new Text(candidate.split(" ")[0].replace(">","").replace("www.",""));
                        context.write(key_num, result);
                        }
                }
            }
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
        // will use traditional TextInputFormat to split line-by-line
        //TextInputFormat.addInputPath(job, new Path(input));
        KeyValueTextInputFormat.addInputPath(job, new Path(input));
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(LinkMapper.class);
        //job.setCombinerClass(LinkReducer.class);
        job.setReducerClass(LinkReducer.class);

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
