import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class PageRankNode {
    double weightOut; // Hubness для HITS
    double weightIn; // Authority для HITS
    ArrayList<String> linksOut = new ArrayList<>();
    ArrayList<String> linksIn = new ArrayList<>();

    public static PageRankNode read(String in, boolean readInputLinks) throws IOException {
        PageRankNode out = new PageRankNode();
        String splitter = "<SPLITTER>";
        String[] data = in.split(splitter);
        out.weightOut = Double.parseDouble(data[0]);
        out.weightIn = Double.parseDouble(data[1]);
        String[] linksInArray;
        String[] linksOutArray;
        if (data[2].equals(" ")) {
            linksOutArray = new String[0];
        } else {
            linksOutArray = data[2].split(" ");
        }

        for (int i = 0; i < linksOutArray.length; i++) {
            if (!linksOutArray[i].equals(" ") & !linksOutArray[i].equals("")) {
                out.linksOut.add(linksOutArray[i]);
            }
        }
        if (readInputLinks) {
            if (data[3].equals(" ")) {
                linksInArray = new String[0];
            } else {
                linksInArray = data[3].split(" ");
            }

            for (int i = 0; i < linksInArray.length; i++) {
                if (!linksInArray[i].equals(" ") & !linksInArray[i].equals("")) {
                    out.linksIn.add(linksInArray[i]);
                }
            }
        }

        return out;
    }

    public Text toText(boolean writeInputLinks) throws IOException {
        StringBuilder outBuilder = new StringBuilder();
        String splitter = "<SPLITTER>";
        outBuilder.append(Double.toString(weightOut)).append(splitter).append(Double.toString(weightIn)).append(splitter);


        StringBuilder linksOutBuilder = new StringBuilder();
        for (String link : linksOut) {
            linksOutBuilder.append(link).append(" ");
        }
        String linksOutStr = linksOutBuilder.toString().trim();

        if (linksOutStr.equals("")) {
            linksOutStr = " ";
        }
        outBuilder.append(linksOutStr);

        if (writeInputLinks) {
            StringBuilder linksInBuilder = new StringBuilder();
            for (String link : linksIn) {
                linksInBuilder.append(link).append(" ");
            }
            String linksInStr = linksInBuilder.toString().trim();
            if (linksInStr.equals("")) {
                linksInStr = " ";
            }
            outBuilder.append(splitter).append(linksInStr);
        }
        return new Text(outBuilder.toString());
    }


    public static long getN(Reducer.Context context, Path unique_file) throws IOException, InterruptedException {
        FileSystem fs = unique_file.getFileSystem(context.getConfiguration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(unique_file)));
        String line = br.readLine();
        long N = Long.parseLong(line.split("\t")[1]);
        br.close();
        return N;
    }

    public static long getN(Mapper.Context context, Path unique_file) throws IOException, InterruptedException {
        FileSystem fs = unique_file.getFileSystem(context.getConfiguration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(unique_file)));
        String line = br.readLine();
        long N = Long.parseLong(line.split("\t")[1]);
        br.close();
        return N;
    }

}