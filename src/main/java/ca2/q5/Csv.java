package ca2.q5;

import java.util.ArrayList;
import java.util.List;

/** Minimal CSV field splitter: handles "quoted, fields" and doubled quotes ("") inside them. */
public final class Csv {

    private Csv() { }

    public static String[] split(String raw) {
        String line = raw;
        int end = line.length();
        while (end > 0 && (line.charAt(end - 1) == '\r' || line.charAt(end - 1) == '\n')) end--;
        line = line.substring(0, end);

        List<String> out = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (quoted) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') { sb.append('"'); i++; }
                    else quoted = false;
                } else {
                    sb.append(c);
                }
            } else if (c == '"' && sb.length() == 0) {
                quoted = true;
            } else if (c == ',') {
                out.add(sb.toString());
                sb.setLength(0);
            } else {
                sb.append(c);
            }
        }
        out.add(sb.toString());
        return out.toArray(new String[0]);
    }
}
