
import java.util.*;

class bigram {
    public static void main(String[] args) {
        String str = "I am sample string and will be tokenized on space";
        ArrayList<String> bigrams = new ArrayList<String>();
        StringTokenizer itr = new StringTokenizer(str);
        if (itr.countTokens() > 1) {
            System.out.println("String array size : " + itr.countTokens());
            String s1 = "";
            String s2 = "";
            String s3 = "";
            while (itr.hasMoreTokens()) {
                if (s1.isEmpty())
                    s1 = itr.nextToken();
                s2 = itr.nextToken();
                s3 = s1 + " " + s2;
                bigrams.add(s3);
                s1 = s2;
                s2 = "";
            }

        } else
            System.out.println("Tokens is 1 or 0");
        int i = 0;
        while (i < bigrams.size()) {
            System.out.println(bigrams.get(i));
            i++;
        }
    }
}

