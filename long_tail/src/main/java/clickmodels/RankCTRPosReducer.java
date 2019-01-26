package clickmodels;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class RankCTRPosReducer {
    public static final String MARKER = "RANKCTR";
    @SuppressWarnings("unchecked")
    public static void reduce(String url,
                              Iterable<Text> values,
                              MultipleOutputs<Text, Text> out) throws IOException, InterruptedException {
        HashMap<Integer, ArrayList<Double>> urlResults = new HashMap<>();
        final String DELIMETER = "::::";
        for (Text i : values) {
            String[] vals = i.toString().split(DELIMETER);
            Integer pos = Integer.parseInt(vals[0]);
            if (!urlResults.containsKey(pos)){
                urlResults.put(pos, new ArrayList<>());
            }
            urlResults.get(pos).add(Double.parseDouble(vals[1]));
        }
        StringBuilder sb = new StringBuilder();
        for (Integer i = 0; i < 10; i++){
            if (!urlResults.containsKey(i)){
                sb.append("-1.0").append("\t");
                continue;
            }
            Double shows = 0.0;
            Double clicks = 0.0;
            for (Double val: urlResults.get(i)){
                if (val > 0.5){
                    clicks += 1;
                }else{
                    shows += 1;
                }
            }
            sb.append(Double.toString(clicks/shows)).append("\t");
        }
        sb.delete(sb.length() - 1, sb.length());
        out.write("xgb", new Text(url), new Text(sb.toString()), "RankCTR");

    }
}
