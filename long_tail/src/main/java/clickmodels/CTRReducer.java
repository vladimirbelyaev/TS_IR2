package clickmodels;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.HashMap;

public class CTRReducer {
    public static final String MARKER = "CTR";

    @SuppressWarnings("unchecked")
    public static void reduce(String query, String url,
                              Iterable<Text> values,
                              MultipleOutputs<Text, Text> out) throws IOException, InterruptedException {

        final String DELIMETER = "::::";
        Double clicks = 0.0;
        Double shows = 0.0;
        for (Text i : values) {
            String[] vals = i.toString().split(DELIMETER);
            shows += Double.parseDouble(vals[0]);
            clicks += Double.parseDouble(vals[1]);
        }
        String k = query + "\t" + url;
        String v = shows.toString() + "\t" + clicks.toString();
        out.write("xgb", new Text(k), new Text(v), "CTR");

    }

}
