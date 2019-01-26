package clickmodels;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.HashMap;

public class FastSDBNReducer {
    public static final String MARKER = "SDBN";
    @SuppressWarnings("unchecked")
    public static void reduce(String query, String url,
                              Iterable<Text> values,
                              MultipleOutputs<Text, Text> out) throws IOException, InterruptedException {

        final String DELIMETER = "::::";
        Double a_D = 0.0;
        Double a_N = 0.0;
        Double s_D = 0.0;
        Double s_N = 0.0;
        for (Text i : values) {
            //System.out.println("Curr string:" + i.toString());
            String[] vals = i.toString().split(DELIMETER);
            a_N += Double.parseDouble(vals[0]);
            a_D += Double.parseDouble(vals[1]);
            s_N += Double.parseDouble(vals[2]);
            s_D += Double.parseDouble(vals[3]);
        }
        String k = query + "\t" + url;
        String v = a_N.toString() + "\t" + a_D.toString() + "\t" + s_N.toString() + "\t" + s_D.toString();
        out.write("xgb", new Text(k), new Text(v), "SBDN");
    }
}
