import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

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
public class GetLinksHITSJob extends Configured implements Tool {


    public static class LinkHITSMapper extends Mapper<Text, Text, Text, Text> {
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
            String keyString = idToPage.get(key.toString());
            Text key_num = new Text(keyString);


            while(match.find()){
                String subSplit = match.group(0).split("href=")[1];
                String[] finalSplit = subSplit.split("\"");
                if (finalSplit.length > 1){
                    String candidate = finalSplit[1];
                    if (!candidate.contains("http")){
                        candidate = "http://lenta.ru" + candidate;
                    }
                    candidate = candidate.replace("www.","");
                    if (candidate.contains("lenta")){
                        context.write(key_num, new Text("O" + candidate)); // From, to
                        context.write(new Text(candidate), new Text("I" + keyString)); // To, from
                    }
                }
                else{
                    String candidate = subSplit.trim();
                    if (candidate.contains("http") & candidate.contains("lenta")){
                        candidate = candidate.split(" ")[0].replace(">","").replace("www.","");
                        context.write(key_num, new Text("O" + candidate)); // From, to
                        context.write(new Text(candidate), new Text("I" + keyString)); // To, from
                    }
                    else if (!candidate.contains("http") & candidate.contains("www.") & candidate.contains("lenta")){
                        candidate = "http://" + candidate;
                        candidate = candidate.split(" ")[0].replace(">","").replace("www.","");
                        context.write(key_num, new Text("O" + candidate)); // From, to
                        context.write(new Text(candidate), new Text("I" + keyString)); // To, from
                    }
                }
            }
        }
    }


    public static class LinkHITSReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> text, Context context) throws IOException, InterruptedException {
            String keyStringFixed = key.toString().replaceAll("\n", " ").replaceAll("\\s+", " ");
            Text keyFixed = new Text(keyStringFixed);
            HashMap<String, Boolean> UniqueLinksIn = new HashMap<>();
            HashMap<String, Boolean> UniqueLinksOut = new HashMap<>();
            StringBuilder linksIn = new StringBuilder();
            StringBuilder linksOut = new StringBuilder();
            for (Text i:text){
                String link = i.toString();
                System.out.println(link);
                System.out.println(link.substring(0,1));
                if (link.substring(0,1).equals("I") & !UniqueLinksIn.containsKey(link.substring(1))){ // Pages with links to the current one
                    linksIn.append(link.substring(1)).append(" ");
                    UniqueLinksIn.put(link.substring(1),Boolean.TRUE);
                }
                else if (link.substring(0,1).equals("O") & !UniqueLinksOut.containsKey(link.substring(1))){ // Links from current page
                    linksOut.append(link.substring(1)).append(" ");
                    UniqueLinksOut.put(link.substring(1),Boolean.TRUE);
                }
            }
            String linksInString = linksIn.toString().trim();
            if (linksInString.equals("")){
                linksInString = " ";
            }
            String linksOutString = linksOut.toString().trim();
            if (linksOutString.equals("")){
                linksOutString = " ";
            }
            String splitter = "<SPLITTER>";
            String value = "1.0" + splitter + "1.0" + splitter + linksOutString + splitter + linksInString;
            value = value.replaceAll("\n", " ").replaceAll("\\s+", " ");
            context.write(keyFixed, new Text(value));

        }
    }

    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(GetLinksHITSJob.class);
        job.setJobName(GetLinksHITSJob.class.getCanonicalName());
        // will use traditional TextInputFormat to split line-by-line
        //TextInputFormat.addInputPath(job, new Path(input));
        KeyValueTextInputFormat.addInputPath(job, new Path(input));
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(LinkHITSMapper.class);
        job.setReducerClass(LinkHITSReducer.class);
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
        int ret = ToolRunner.run(new GetLinksHITSJob(), args);
        System.exit(ret);
    }
}