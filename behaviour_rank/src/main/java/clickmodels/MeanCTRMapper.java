package clickmodels;

import SERPLog.SERPLog;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

public class MeanCTRMapper {
    private final String DELIMETER = "::::";
    private final String MARKER = "MEANCTR" + DELIMETER;
    private HashMap<String, Long> urlMap;

    public MeanCTRMapper(HashMap<String, Long> urlMap) {
        this.urlMap = urlMap;
    }
    @SuppressWarnings("unchecked")
    public void map(Mapper.Context context, SERPLog log) throws IOException, InterruptedException {
        HashMap<String, Double> isClicked = new HashMap<>();
        for (int clickedPosition : log.clickedPositions) {
            String link = log.shownLinks[clickedPosition];
            isClicked.put(link, 1.0);

        }
        for (String link : log.shownLinks) {
            if (urlMap.containsKey(link)) {
                context.write(new Text(MARKER + link), new Text(log.query + DELIMETER + Double.toString(isClicked.getOrDefault(link, 0.0))));
            }
        }

    }
}
