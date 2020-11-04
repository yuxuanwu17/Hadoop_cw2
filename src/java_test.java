import org.apache.jute.compiler.JString;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class java_test {

    public static String text = "Let's play WE're best,\n" +
            "THIS ELECTRONIC VERSION OF THE ????COMPLETE WORKS OF WILLIAM\n" +
            "SHAKESPEARE IS COPYRIGHT 1990-1993 BY WORLD LIBRARY, INC., AND IS\n" +
            "PROVIDED BY PROJECT GUTENBERG ETEXT OF ILLINOIS BENEDICTINE COLLEGE\n" +
            "WITH PERMISSION.  ELECTRONIC AND MACHINE READABLE COPIES MAY BE\n" +
            "DISTRIBUTED SO LONG AS SUCH COPIES (1) ARE FOR YOUR OR OTHERS\n" +
            "PERSONAL USE ONLY, AND (2) ARE NOT DISTRIBUTED OR USED\n" +
            "COMMERCIALLY.  PROHIBITED COMMERCIAL DISTRIBUTION INCLUDES BY ANY\n" +
            "SERVICE THAT CHARGES FOR DOWNLOAD TIME OR FOR MEMBERSHIP";

    public static String deleteNotation(String words) {
        String regEx = "[`~!@#$%^&*()+=|{}':;',\\[\\].-<>/?~@#%……&*--+|{}\"'[0-9]]";
        Pattern p = Pattern.compile(regEx);
        Matcher matcher = p.matcher(words);
        return matcher.replaceAll("").trim();
    }


    public static void main(String[] args) {
//        String del = " DELIMITER ";
//        String del = "";
        ArrayList<String> bigrams = new ArrayList<String>();
        ArrayList<String> processed_data = new ArrayList<String>();
        StringTokenizer itr = new StringTokenizer(text);
        String s1 = "";
        String s2 = "";
        String s3 = "";
        while (itr.hasMoreTokens()) {
            String s = itr.nextToken();
            s = s.toLowerCase();
            s = deleteNotation(s);
            processed_data.add(s);
        }

        for (String datum : processed_data) {
            System.out.println(datum);
        }

    }
}
