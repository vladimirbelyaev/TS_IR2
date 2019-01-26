package clickmodels;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;

// SDBN, сокращающий действия при использовании train.marks и sample
public class FastSDBNMapper {
    private final String DELIMETER = "::::";
    private final String MARKER = "SDBN" + DELIMETER;

    public FastSDBNMapper() {}

    @SuppressWarnings("unchecked")
    public void map(Mapper.Context context, SERPLog log) throws IOException, InterruptedException {
        HashMap<String, Double> a_D = new HashMap<>();
        HashMap<String, Double> a_N = new HashMap<>();
        HashMap<String, Double> s_D = new HashMap<>();
        HashMap<String, Double> s_N = new HashMap<>();
        for (String link : log.shownLinks) {
        }
        int lastClickedUrl = log.clickedPositions[log.clickedPositions.length - 1];
        for (int i = 0; i < log.shownLinks.length; i++) {
            String link = log.shownLinks[i];
            if (i <= lastClickedUrl) {
                a_D.put(link, 1.0);
            }
        }
        for (int clickedPosition : log.clickedPositions) {
            String link = log.shownLinks[clickedPosition];
                a_N.put(link, 1.0);
                s_D.put(link, 1.0);
        }
        s_N.put(log.shownLinks[lastClickedUrl], 1.0);
        for (int i=0; i < log.shownLinks.length; i++) {
            if (log.markedLinks[i]) {
                String link = log.shownLinks[i];
                context.write(new Text(MARKER + log.query + DELIMETER + link), new Text(Double.toString(a_N.getOrDefault(link, 0.0)) +
                        DELIMETER + Double.toString(a_D.getOrDefault(link, 0.0)) +
                        DELIMETER + Double.toString(s_N.getOrDefault(link, 0.0)) +
                        DELIMETER + Double.toString(s_D.getOrDefault(link, 0.0))));
            }
        }

    }
}

