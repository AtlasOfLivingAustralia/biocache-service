
package au.org.ala.names.util;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;

/**
 * A Java implementation of the sound ex algorithm supplied by Tony Rees
 *
 * @author trobertson
 *         <p/>
 *         Copied from Taxamatch project. We don't need full taxamatch...
 */
public class TaxonNameSoundEx {


    /* remove leading whitespace */
    public static String ltrim(String source) {
        return source.replaceAll("^\\s+", "");
    }

    /* remove trailing whitespace */
    public static String rtrim(String source) {
        return source.replaceAll("\\s+$", "");
    }

    private static String translate(String source, String transSource, String transTarget) {
        String result = source;

        while (transSource.length() > transTarget.length()) {

            transTarget += " ";

        }
        for (int i = 0; i < transSource.length(); i++) {

            result = result.replace(transSource.charAt(i), transTarget.charAt(i));

        }
        return result;
    }


    public static String normalize(String str) {

        if (str == null) return null;

        String output = str;

        // trim any leading, trailing spaces or line feeds
        //output = ltrim(rtrim(str));

        output = output.replace(" cf ", " ");
        output = output.replace(" cf. ", " ");
        output = output.replace(" near ", " ");
        output = output.replace(" aff. ", " ");
        output = output.replace(" sp.", " ");
        output = output.replace(" spp.", " ");
        output = output.replace(" spp ", " ");

        output = str.toUpperCase();

        // replace any HTML ampersands
        output = output.replace(" &AMP; ", " & ");

        // remove any content in angle brackets (e.g. html tags - <i>, </i>, etc.)
        output = output.replaceAll("\\<.+?\\>", "");

        output = translate(output, "\u00c1\u00c9\u00cd\u00d3\u00da\u00c0\u00c8\u00cc\u00d2\u00d9" +
                "\u00c2\u00ca\u00ce\u00d4\u00db\u00c4\u00cb\u00cf\u00d6\u00dc\u00c3\u00d1\u00d5" +
                "\u00c5\u00c7\u00d8", "AEIOUAEIOUAEIOUAEIOUANOACO");

        output = output.replace("\u00c6", "AE");
        output = output.replaceAll("[^a-zA-Z .]", "");
        output = StringUtils.trimToNull(output);

        return output;
    }


    public static String treatWord(String str2, String wordType) {
        char startLetter;
        String temp = normalize(str2);
        // Do some selective replacement on the leading letter/s only:
        if (StringUtils.isNotEmpty(temp)) {
            if (temp.startsWith("AE")) {
                temp = "E" + temp.substring(2);
            } else if (temp.startsWith("CN")) {
                temp = "N" + temp.substring(2);
            } else if (temp.startsWith("CT")) {
                temp = "T" + temp.substring(2);
            } else if (temp.startsWith("CZ")) {
                temp = "C" + temp.substring(2);
            } else if (temp.startsWith("DJ")) {
                temp = "J" + temp.substring(2);
            } else if (temp.startsWith("EA")) {
                temp = "E" + temp.substring(2);
            } else if (temp.startsWith("EU")) {
                temp = "U" + temp.substring(2);
            } else if (temp.startsWith("GN")) {
                temp = "N" + temp.substring(2);
            } else if (temp.startsWith("KN")) {
                temp = "N" + temp.substring(2);
            } else if (temp.startsWith("MC")) {
                temp = "MAC" + temp.substring(2);
            } else if (temp.startsWith("MN")) {
                temp = "N" + temp.substring(2);
            } else if (temp.startsWith("OE")) {
                temp = "E" + temp.substring(2);
            } else if (temp.startsWith("QU")) {
                temp = "Q" + temp.substring(2);
            } else if (temp.startsWith("PS")) {
                temp = "S" + temp.substring(2);
            } else if (temp.startsWith("PT")) {
                temp = "T" + temp.substring(2);
            } else if (temp.startsWith("TS")) {
                temp = "S" + temp.substring(2);
            } else if (temp.startsWith("WR")) {
                temp = "R" + temp.substring(2);
            } else if (temp.startsWith("X")) {
                temp = "Z" + temp.substring(2);
            }
            // Now keep the leading character, then do selected "soundalike" replacements. The
            // following letters are equated: AE, OE, E, U, Y and I; IA and A are equated;
            // K and C; Z and S; and H is dropped. Also, A and O are equated, MAC and MC are equated, and SC and S.
            startLetter = temp.charAt(0); // quarantine the leading letter
            temp = temp.substring(1); // snip off the leading letter
            // now do the replacements
            temp = temp.replaceAll("AE", "I");
            temp = temp.replaceAll("IA", "A");
            temp = temp.replaceAll("OE", "I");
            temp = temp.replaceAll("OI", "A");
            temp = temp.replaceAll("SC", "S");
            temp = temp.replaceAll("E", "I");
            temp = temp.replaceAll("O", "A");
            temp = temp.replaceAll("U", "I");
            temp = temp.replaceAll("Y", "I");
            temp = temp.replaceAll("K", "C");
            temp = temp.replaceAll("Z", "C");
            temp = temp.replaceAll("H", "");
            // add back the leading letter
            temp = startLetter + temp;
            // now drop any repeated characters (AA becomes A, BB or BBB becomes B, etc.)
            temp = temp.replaceAll("(\\w)\\1+", "$1");

            if (wordType == "species") {
                if (temp.endsWith("IS")) {
                    temp = temp.substring(0, temp.length() - 2) + "A";
                } else if (temp.endsWith("IM")) {
                    temp = temp.substring(0, temp.length() - 2) + "A";
                } else if (temp.endsWith("AS")) {
                    temp = temp.substring(0, temp.length() - 2) + "A";
                }
                //temp = temp.replaceAll("(\\w)\\1+", "$1");
            }
        }
        return temp;
    }


    /**
     * Returns the SoundEx for the source string
     *
     * @param source String to get the sound ex of
     * @return The sound ex string
     */
    public String soundEx(String source) {
        String temp = source.toUpperCase();
        temp = selectiveReplaceFirstChar(temp);
        temp = selectiveReplaceWithoutFirstChar(temp);
        temp = removeRepeatedChars(temp);
        temp = alphabetiseWordsIgnoringFirstLetter(temp);

        return temp;
    }

    /**
     * Ignoring the first letter, alphabetise each word
     */
    String alphabetiseWordsIgnoringFirstLetter(String source) {
        StringTokenizer st = new StringTokenizer(source, " ");
        StringBuffer sb = new StringBuffer();
        while (st.hasMoreTokens()) {
            String token = st.nextToken();
            char[] chars = token.toCharArray();
            List<Character> charList = new LinkedList<Character>();
            for (int i = 1; i < chars.length; i++) {
                charList.add(chars[i]);
            }
            Collections.sort(charList);
            sb.append(chars[0]);
            for (Character c : charList) {
                sb.append(c);
            }
            if (st.hasMoreTokens()) {
                sb.append(" ");
            }
        }
        return sb.toString();
    }

    /**
     * Removes repeated characters
     * Can't get the regex version working so pretty primitive...
     */
    String removeRepeatedChars(String source) {
        StringBuffer sb = new StringBuffer();
        char c = ' ';
        for (int i = 0; i < source.length(); i++) {
            char sourceC = source.charAt(i);
            if (sourceC != c) {
                sb.append(sourceC);
            }
            c = sourceC;
        }
        return sb.toString();
    }

    /**
     * Ignoring the first character, selectively replace sound alikes
     */
    String selectiveReplaceWithoutFirstChar(String source) {
        if (source.length() > 1) {
            String temp = source.substring(1);
            temp = temp.replaceAll("AE", "I");
            temp = temp.replaceAll("IA", "A");
            temp = temp.replaceAll("OE", "I");
            temp = temp.replaceAll("OI", "A");
            temp = temp.replaceAll("MC", "MAC");
            temp = temp.replaceAll("SC", "S");
            temp = temp.replaceAll("EOUYKZH", "IAIICS");

            return source.substring(0, 1) + temp;
        } else {
            return source;
        }
    }

    /**
     * Selectively replaces the first character
     */
    String selectiveReplaceFirstChar(String source) {
        if (source.startsWith("Æ")) {
            return source.replaceFirst("Æ", "E");

        } else if (source.startsWith("AE")) {
            return source.replaceFirst("AE", "E");

        } else if (source.startsWith("CN")) {
            return source.replaceFirst("CN", "N");

        } else if (source.startsWith("CT")) {
            return source.replaceFirst("CT", "T");

        } else if (source.startsWith("CZ")) {
            return source.replaceFirst("CZ", "C");

        } else if (source.startsWith("DJ")) {
            return source.replaceFirst("DJ", "J");

        } else if (source.startsWith("EA")) {
            return source.replaceFirst("EA", "E");

        } else if (source.startsWith("EU")) {
            return source.replaceFirst("EU", "U");

        } else if (source.startsWith("GN")) {
            return source.replaceFirst("GN", "N");

        } else if (source.startsWith("KN")) {
            return source.replaceFirst("KN", "N");

        } else if (source.startsWith("MN")) {
            return source.replaceFirst("MN", "N");

        } else if (source.startsWith("OE")) {
            return source.replaceFirst("OE", "E");

        } else if (source.startsWith("QU")) {
            return source.replaceFirst("QU", "Q");

        } else if (source.startsWith("PS")) {
            return source.replaceFirst("PS", "S");

        } else if (source.startsWith("PT")) {
            return source.replaceFirst("PT", "T");

        } else if (source.startsWith("TS")) {
            return source.replaceFirst("TS", "S");

        } else if (source.startsWith("X")) {
            return source.replaceFirst("X", "Z");

        } else return source;
    }
}