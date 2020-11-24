import org.apache.jute.compiler.JString;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class bigrams {

    public static String text = "Let's play WE're best,\n" +
            "THIS ELECTRONIC VERSION OF THE ????COMPLETE WORKS OF WILLIAM\n" +
            "SHAKESPEARE IS COPYRIGHT 1990-1993 BY WORLD LIBRARY, INC., AND IS\n" +
            "PROVIDED BY PROJECT GUTENBERG ETEXT OF ILLINOIS BENEDICTINE COLLEGE\n" +
            "WITH PERMISSION.  ELECTRONIC AND MACHINE READABLE COPIES MAY BE\n" +
            "DISTRIBUTED SO LONG AS SUCH COPIES (1) ARE FOR YOUR OR OTHERS\n" +
            "PERSONAL USE ONLY, AND (2) ARE NOT DISTRIBUTED OR USED\n" +
            "COMMERCIALLY.  PROHIBITED COMMERCIAL DISTRIBUTION INCLUDES BY ANY\n" +
            "SERVICE THAT CHARGES FOR DOWNLOAD TIME OR FOR MEMBERSHIP let's we'll";

    public static String deleteNotation(String words) {
        String regEx = "[\\W[0-9]&&[^\']]";
        Pattern p = Pattern.compile(regEx);
        Matcher matcher = p.matcher(words);
        return matcher.replaceAll("").trim();
    }


    public static void main(String[] args) {
        ArrayList<String> bigrams = new ArrayList<String>();
        String[] single_word = text.split("\\s+");

//        for (String s : single_word) {
//            String processed = deleteNotation(s);
//            System.out.println(processed);
//        }

        for (int i = 0; i < single_word.length - 1; i++) {
            // note that, in this case we should constrain the text length into length -1
            // since this is the bigram case, there is length - 1 bigram words in total
            // otherwise, it could case break the string
            single_word[i] = deleteNotation(single_word[i]);
            single_word[i + 1] = deleteNotation(single_word[i + 1]);
            if (!(single_word[i].isEmpty()) && !(single_word[i + 1].isEmpty())) {
                bigrams.add(single_word[i] + " " + single_word[i + 1]);
            }

        }

        for (String datum : bigrams) {
            System.out.println(datum);
        }

    }
}


