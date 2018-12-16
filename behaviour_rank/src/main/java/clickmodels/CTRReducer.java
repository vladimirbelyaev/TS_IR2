package clickmodels;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.HashMap;

public class CTRReducer {
    public static final String MARKER = "CTR";
    @SuppressWarnings("unchecked")
    public static void reduce(String queryurl,
                              Iterable<Text> values,
                              HashMap<String, Long> trainMarks,
                              HashMap<String, String> sampleNames,
                              MultipleOutputs<Text, Text> out) throws IOException, InterruptedException {

        final String DELIMETER = "::::";
        Double clicks = 0.0;
        Double shows = 0.0;
        System.out.println(queryurl);
        for (Text i : values) {
            String[] vals = i.toString().split(DELIMETER);
            shows += Double.parseDouble(vals[0]);
            clicks += Double.parseDouble(vals[1]);
        }
        boolean flag = true;
        if (trainMarks.containsKey(queryurl)) {
            out.write("xgb", new Text(queryurl + "\t" + shows.toString() + " " + clicks.toString()), new Text(trainMarks.get(queryurl).toString()), "CTRtrain");
            flag = false;
        }
        if (sampleNames.containsKey(queryurl)) {
            out.write("xgb", new Text(shows.toString() + " " + clicks.toString()), new Text(sampleNames.get(queryurl)), "CTRtest");
            flag = false;
        }
        if (flag) {
            throw new InterruptedException("Some problems in reducer");
        }
    }
}
