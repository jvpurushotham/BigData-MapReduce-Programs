package ca2.q3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * products.csv (product_id, product_name, category) loaded from the Distributed Cache into a HashMap in
 * setup(): the "small side" of a REPLICATED (map-side) join. Used by the Job 2 mapper and the Job 3 reducer.
 */
public final class ProductCatalog {

    public static final String UNMAPPED = "UNMAPPED";

    private final Map<String, String[]> byId = new HashMap<>();   // product_id -> {name, category}

    public static ProductCatalog load(TaskAttemptContext ctx) throws IOException {
        ProductCatalog cat = new ProductCatalog();
        URI[] cached = ctx.getCacheFiles();
        if (cached == null || cached.length == 0) throw new IOException("products.csv was not added to the Distributed Cache");
        for (URI uri : cached) {
            Path p = new Path(uri);
            FileSystem fs = p.getFileSystem(ctx.getConfiguration());
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.trim().isEmpty()) continue;
                    String[] f = Csv.split(line);
                    if (f[0].trim().equalsIgnoreCase("product_id")) continue;            // header
                    if (f.length < 3 || f[0].trim().isEmpty()) {
                        ctx.getCounter(Q3Counter.PRODUCT_ROWS_MALFORMED).increment(1);
                        continue;
                    }
                    String id = f[0].trim();
                    String category = f[2].trim().isEmpty() ? UNMAPPED : f[2].trim();
                    if (cat.byId.containsKey(id)) {
                        ctx.getCounter(Q3Counter.DUPLICATE_PRODUCT_ROWS).increment(1);   // first row wins
                        continue;
                    }
                    cat.byId.put(id, new String[] { f[1].trim(), category });
                    ctx.getCounter(Q3Counter.PRODUCTS_LOADED).increment(1);
                }
            }
        }
        return cat;
    }

    public String category(String productId) {
        String[] e = byId.get(productId);
        return e == null ? UNMAPPED : e[1];
    }

    public boolean known(String productId) {
        return byId.containsKey(productId);
    }

    public String name(String productId) {
        String[] e = byId.get(productId);
        return e == null ? "(unknown)" : e[0];
    }
}
