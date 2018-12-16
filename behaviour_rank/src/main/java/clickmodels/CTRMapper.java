package clickmodels;

import SERPLog.SERPLog;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

public class CTRMapper {
    private final String DELIMETER = "::::";
    private final String MARKER = "CTR" + DELIMETER;
    HashSet<String> inTrainURLs;
    HashSet<String> inSampleURLs;

    public CTRMapper(HashSet<String> inTrainURLs, HashSet<String> inSampleURLs) {
        this.inSampleURLs = inSampleURLs;
        this.inTrainURLs = inTrainURLs;
    }

    @SuppressWarnings("unchecked")
    public void map(Mapper.Context context, SERPLog log) throws IOException, InterruptedException {
        HashMap<String, Double> shows = new HashMap<>();
        HashMap<String, Double> clicks = new HashMap<>();
        HashSet<String> goodLinks = new HashSet<>();
        for (String link : log.shownLinks) {
            if (inTrainURLs.contains(log.query + " " + link) | inSampleURLs.contains(log.query + " " + link)) {
                goodLinks.add(link);
                shows.put(link, 1.0);
            }
        }
        for (int clickedPosition : log.clickedPositions) {
            String link = log.shownLinks[clickedPosition];
            if (goodLinks.contains(link)) {
                clicks.put(link, 1.0);
            }
        }
        for (String link : goodLinks) {
            context.write(new Text(MARKER + log.query + " " + link), new Text(Double.toString(shows.getOrDefault(link, 0.0)) +
                    DELIMETER + Double.toString(clicks.getOrDefault(link, 0.0))));
        }

    }
}
