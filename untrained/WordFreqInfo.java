import java.util.*;
import java.io.Serializable;

public class WordFreqInfo implements Serializable {
    private static final long serialVersionUID = 1L;
    private String word;
    private int occurCount;
    private ArrayList<Frequency> followList;

    public WordFreqInfo(String word, int count) {
        this.word = word;
        this.occurCount = count;
        this.followList = new ArrayList<Frequency>();
    }

    public String getFollowWord(int count){
        int total = 0;
        for (Frequency f : followList) {
            total += f.followCount;
            if (total > count){
                return f.follow;
            }
        } return " ";
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Word :" + word+":");
        sb.append(" (" + occurCount + ") : ");
        for (Frequency f : followList) {
            sb.append(f.toString());
        }

        return sb.toString();
    }

    public void updateFollows(String follow) {
        this.occurCount++;
        boolean updated = false;
        for (Frequency f : followList) {
            if (follow.compareTo(f.follow) == 0) {
                f.followCount++;
                updated = true;
            }
        }
        if (!updated) {
            followList.add(new Frequency(follow, 1));
        }
    }

    public int getOccurCount() {
        return this.occurCount;
    }

    private static class Frequency implements Serializable {
        private static final long serialVersionUID = 1L;
        String follow;
        int followCount;

        public Frequency(String follow, int ct) {
            this.follow = follow;
            this.followCount = ct;
        }

        @Override
        public String toString() {
            return follow + " [" + followCount + "] ";
        }

        @Override
        public boolean equals(Object f2) {
            return this.follow.equals(((Frequency)f2).follow);
        }
    }
}
