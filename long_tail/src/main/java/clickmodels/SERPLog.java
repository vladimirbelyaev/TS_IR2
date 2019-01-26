package clickmodels;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

public class SERPLog {
    public String query;
    public Long geo;
    public String[] shownLinks;
    public int[] clickedPositions;
    public long[] timestamps;
    public boolean[] markedLinks;

    static String[] filterLinks(String str, HashSet<String> specialLinks, boolean possibleComma){
        String[] possibleLinks = str.split(",https?://");
        if (possibleLinks[0].startsWith("http://")){
            possibleLinks[0] = possibleLinks[0].substring(7);
        }
        if (possibleLinks[0].startsWith("https://")){
            possibleLinks[0] = possibleLinks[0].substring(8);
        }
        ArrayList<String> linksArray = new ArrayList<>();
        // Что может случиться? На самом деле, только ситуация real_link,special1,special2... Надо это разбить.
        for (String link: possibleLinks) {
            if (!link.contains(",")) {
                linksArray.add(link);
            } else {
                if (!link.substring(0, link.length() - 1).contains(",") & possibleComma) {
                    linksArray.add(link.substring(0, link.length() - 1));
                    continue;
                }
                int curr_pos = linksArray.size();
                String[] possibleSplits = link.split(",");
                for (int i = possibleSplits.length - 1; i >= 0; i--) {
                    String possibleSpecial = possibleSplits[i];
                    if (specialLinks.contains(possibleSpecial) | possibleSpecial.startsWith("spritze.")) {
                        linksArray.add(curr_pos, possibleSpecial);
                    } else {
                        String leftLink = String.join(",", Arrays.copyOfRange(possibleSplits, 0, i + 1));
                        if (leftLink.substring(leftLink.length() - 1).equals(",") & possibleComma) {
                            linksArray.add(curr_pos, leftLink.substring(0, leftLink.length() - 1));
                            break;
                        } else {
                            linksArray.add(curr_pos, leftLink);
                            break;
                        }
                    }
                }
            }
        }
        return linksArray.toArray(new String[0]);
    }

    public SERPLog(String val, HashSet<String> urlSet, HashSet<String> specialLinks) throws InterruptedException{
        super();
        String[] valArray = val.trim().split("\t");

        String[] queryAndGeo = valArray[0].split("@");
        this.query = queryAndGeo[0];
        this.geo = Long.parseLong(queryAndGeo[1]);
        this.shownLinks = filterLinks(valArray[1], specialLinks, true);
        String[] clickedLinks = filterLinks(valArray[2], specialLinks, false);
        this.clickedPositions = new int[clickedLinks.length];
        for (int i = 0; i < clickedLinks.length; i++){
            String currlink = clickedLinks[i];
            boolean found = false;
            for (int j = 0; j < shownLinks.length; j++){
                if (currlink.equals(shownLinks[j])){
                    this.clickedPositions[i] = j;
                    found = true;
                    break;
                }
            }
            if (!found) {
                System.out.println(
                        "Clicked link not found in shown links. Please check your parser.\nClicked:\n" + currlink
                                + "\nShown:\n" + String.join("\n", this.shownLinks) + "\n" + val);
            }
        }
        String[] timestamp_arr = valArray[3].split(",");
        this.timestamps = new long[timestamp_arr.length];
        for (int i=0; i<timestamp_arr.length; i++){
            this.timestamps[i] = Long.parseLong(timestamp_arr[i]);
        }
        this.markedLinks = new boolean[this.shownLinks.length];
        for (int i=0; i<shownLinks.length; i++){
            this.markedLinks[i] = urlSet.contains(this.shownLinks[i]);
        }
    }
}
