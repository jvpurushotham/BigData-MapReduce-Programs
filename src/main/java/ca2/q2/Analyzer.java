package ca2.q2;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Shared parsing and tokenising rules for Q2, so that Job 1 (count N), Job 2 (index) and Job 3 (phrase)
 * can never disagree about what a document or a token is.
 */
public final class Analyzer {

    private Analyzer() { }

    /** One parsed corpus line: doc_id TAB title TAB body. */
    public static final class Doc {
        public String id;
        public String title;
        public String body;
        public boolean extraFields;
    }

    /**
     * @return the parsed document, or null if the line is malformed (fewer than 2 fields, empty doc_id).
     *         A line with only "doc_id TAB title" (no body column) is a valid document with an EMPTY body.
     */
    public static Doc parse(String raw) {
        String line = raw;
        int end = line.length();
        while (end > 0 && (line.charAt(end - 1) == '\r' || line.charAt(end - 1) == '\n')) end--;
        line = line.substring(0, end);

        String[] f = line.split("\t", -1);
        if (f.length < 2) return null;
        String id = f[0].trim();
        if (id.isEmpty()) return null;

        Doc d = new Doc();
        d.id = id;
        d.title = f[1];
        if (f.length == 2) {
            d.body = "";
        } else if (f.length == 3) {
            d.body = f[2];
        } else {                                   // stray tabs inside the body: keep the text, flag it
            StringBuilder sb = new StringBuilder(f[2]);
            for (int i = 3; i < f.length; i++) sb.append(' ').append(f[i]);
            d.body = sb.toString();
            d.extraFields = true;
        }
        return d;
    }

    /** Lower-case and split on every non-alphanumeric character. Returns ALL alphanumeric runs (no filtering). */
    public static List<String> tokens(String text) {
        List<String> out = new ArrayList<>();
        String s = text.toLowerCase(Locale.ROOT);
        int n = s.length();
        int i = 0;
        while (i < n) {
            while (i < n && !Character.isLetterOrDigit(s.charAt(i))) i++;
            int start = i;
            while (i < n && Character.isLetterOrDigit(s.charAt(i))) i++;
            if (i > start) out.add(s.substring(start, i));
        }
        return out;
    }
}
