package SERPLog;

public class SERPLogUnhashed extends SERPLog {
    public SERPLogUnhashed(String val) {
        super();
        String[] valArray = val.trim().split("\t");

        String[] queryAndGeo = valArray[0].split("@");
        this.query = queryAndGeo[0];
        this.geo = Long.parseLong(queryAndGeo[1]);


        String[] shownLinksUnhashed = valArray[1].replace("http://","").replace("https://","").split(",");
        this.shownLinks = new String[shownLinksUnhashed.length];
        for (int i = 0; i < shownLinksUnhashed.length; i++){
            this.shownLinks[i] = shownLinksUnhashed[i];
        }

        String[] clickedLinks = valArray[2].replace("http://","").replace("https://","").split(",");
        this.clickedPositions = new int[clickedLinks.length];
        for (int i = 0; i < clickedLinks.length; i++){
            String currlink = clickedLinks[i];
            for (int j = 0; j < shownLinksUnhashed.length; j++){
                if (currlink.equals(shownLinksUnhashed[j])){
                    this.clickedPositions[i] = j;
                }
            }
        }

        String[] timestamp_arr = valArray[3].split(",");
        this.timestamps = new long[timestamp_arr.length];
        for (int i=0; i<timestamp_arr.length; i++){
            this.timestamps[i] = Long.parseLong(timestamp_arr[i]);
        }
    }
}
