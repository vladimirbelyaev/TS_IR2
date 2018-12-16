package clickmodels;

import SERPLog.SERPLog;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

// SDBN, сокращающий действия при использовании train.marks и sample
public class FastSDBNMapper {
    private final String DELIMETER = "::::";
    private final String MARKER = "SDBN" + DELIMETER;
    HashSet<String> inTrainURLs;
    HashSet<String> inSampleURLs;

    public FastSDBNMapper(HashSet<String> inTrainURLs, HashSet<String> inSampleURLs) {
        this.inSampleURLs = inSampleURLs;
        this.inTrainURLs = inTrainURLs;
    }

    @SuppressWarnings("unchecked")
    public void map(Mapper.Context context, SERPLog log) throws IOException, InterruptedException {
        HashMap<String, Double> a_D = new HashMap<>();
        HashMap<String, Double> a_N = new HashMap<>();
        HashMap<String, Double> s_D = new HashMap<>();
        HashMap<String, Double> s_N = new HashMap<>();
        HashSet<String> goodLinks = new HashSet<>();
        for (String link : log.shownLinks) {

            if (inTrainURLs.contains(log.query + " " + link) | inSampleURLs.contains(log.query + " " + link)) {
                goodLinks.add(link);
            }
        }
        int lastClickedUrl = log.clickedPositions[log.clickedPositions.length - 1];
        for (int i = 0; i < log.shownLinks.length; i++) {
            String link = log.shownLinks[i];
            if (goodLinks.contains(link)) {
                if (i <= lastClickedUrl) {
                    a_D.put(link, 1.0);
                }
            }
        }
        for (int clickedPosition : log.clickedPositions) {
            String link = log.shownLinks[clickedPosition];
            if (goodLinks.contains(link)) {
                a_N.put(link, 1.0);
                s_D.put(link, 1.0);
            }
        }
        if (goodLinks.contains(log.shownLinks[lastClickedUrl])) {
            s_N.put(log.shownLinks[lastClickedUrl], 1.0);
        }

        for (String link : goodLinks) {
            context.write(new Text(MARKER + log.query + " " + link), new Text(Double.toString(a_N.getOrDefault(link, 0.0)) +
                    DELIMETER + Double.toString(a_D.getOrDefault(link, 0.0)) +
                    DELIMETER + Double.toString(s_N.getOrDefault(link, 0.0)) +
                    DELIMETER + Double.toString(s_D.getOrDefault(link, 0.0))));
        }

    }
}

