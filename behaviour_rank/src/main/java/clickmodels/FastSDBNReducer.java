package clickmodels;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.HashMap;

public class FastSDBNReducer {
    public static final String MARKER = "SDBN";
    @SuppressWarnings("unchecked")
    public static void reduce(String queryurl,
                              Iterable<Text> values,
                              HashMap<String, Long> trainMarks,
                              HashMap<String, String> sampleNames,
                              MultipleOutputs<Text, Text> out) throws IOException, InterruptedException {

        final String DELIMETER = "::::";
        Double a_D = 0.0;
        Double a_N = 0.0;
        Double s_D = 0.0;
        Double s_N = 0.0;
        System.out.println(queryurl);
        for (Text i : values) {
            System.out.println("Curr string:" + i.toString());
            String[] vals = i.toString().split(DELIMETER);
            a_N += Double.parseDouble(vals[0]);
            a_D += Double.parseDouble(vals[1]);
            s_N += Double.parseDouble(vals[2]);
            s_D += Double.parseDouble(vals[3]);
        }
        boolean flag = true;
        if (trainMarks.containsKey(queryurl)) {
            out.write("xgb", new Text(queryurl + "\t" + a_N.toString() + " " + a_D.toString() +
                    " " + s_N.toString() + " " + s_D.toString()), new Text(trainMarks.get(queryurl).toString()), "SBDNtrain");
            flag = false;
        }
        if (sampleNames.containsKey(queryurl)) {
            out.write("xgb", new Text(a_N.toString() + " " + a_D.toString() +
                    " " + s_N.toString() + " " + s_D.toString()), new Text(sampleNames.get(queryurl)), "SBDNtest");
            flag = false;
        }
        if (flag) {
            throw new InterruptedException("Some problems in reducer");
        }
    }
}
