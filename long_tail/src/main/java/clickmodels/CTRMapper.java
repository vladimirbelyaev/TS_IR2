package clickmodels;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

public class CTRMapper {
    private final String DELIMETER = "::::";
    private final String MARKER = "CTR" + DELIMETER;

    public CTRMapper() {
    }

    @SuppressWarnings("unchecked")
    public void map(Mapper.Context context, SERPLog log) throws IOException, InterruptedException {
        HashMap<String, Double> shows = new HashMap<>();
        HashMap<String, Double> clicks = new HashMap<>();
        for (String link : log.shownLinks) {
                shows.put(link, 1.0);
        }
        for (int clickedPosition : log.clickedPositions) {
            String link = log.shownLinks[clickedPosition];
                clicks.put(link, 1.0);
        }
        for (int i=0; i < log.shownLinks.length; i++) {
            if (log.markedLinks[i]) {
                String link = log.shownLinks[i];
                context.write(new Text(MARKER + log.query + DELIMETER + link), new Text(Double.toString(shows.getOrDefault(link, 0.0)) +
                        DELIMETER + Double.toString(clicks.getOrDefault(link, 0.0))));
            }
        }

    }
}
