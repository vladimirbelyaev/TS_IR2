package clickmodels;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.HashMap;

public class ShowPosReducer {
    public static final String MARKER = "SHOWPOS";
    @SuppressWarnings("unchecked")
    public static void reduce(String query, String url,
                              Iterable<Text> values,
                              MultipleOutputs<Text, Text> out) throws IOException, InterruptedException {

        Double showPosSum = 0.0;
        int counter = 0;
        for (Text i : values) {
            showPosSum += Double.parseDouble(i.toString());
            counter += 1;
        }
        String k = query + "\t" + url;
        String v = Double.toString(showPosSum/counter);
        out.write("xgb", new Text(k), new Text(v), "ShowPos");
    }
}
