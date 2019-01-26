package clickmodels;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class MeanShowPosReducer {
    public static final String MARKER = "MEANSHOW";
    @SuppressWarnings("unchecked")
    public static void reduce(String url,
                              Iterable<Text> values,
                              MultipleOutputs<Text, Text> out) throws IOException, InterruptedException {
        HashMap<String, ArrayList<Double>> queryResults = new HashMap<>();
        final String DELIMETER = "::::";
        for (Text i : values) {
            String[] vals = i.toString().split(DELIMETER);
            if (!queryResults.containsKey(vals[0])){
                queryResults.put(vals[0], new ArrayList<>());
            }
            queryResults.get(vals[0]).add(Double.parseDouble(vals[1]));
        }
        Double CTRfullsum = 0.0;
        Double CTRmeansum = 0.0;
        int CTRcounter = 0;
        for (Map.Entry<String, ArrayList<Double>> entry: queryResults.entrySet()){
            double currSum = 0;
            for (Double isClicked: entry.getValue()){
                CTRcounter += 1;
                currSum += isClicked;
                CTRfullsum += isClicked;
            }
            CTRmeansum += currSum/entry.getValue().size();
        }
        CTRfullsum /= CTRcounter;
        CTRmeansum /= queryResults.size();
        out.write("xgb", new Text(url), new Text(CTRfullsum.toString() + "\t" + CTRmeansum.toString()), "MeanShowPos");

    }
}
