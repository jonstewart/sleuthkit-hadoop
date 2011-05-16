package org.sleuthkit.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;

// Maps regex matches to an output file.
public class GrepMapper
extends SKMapper<Text, Text, Text, Text> {

    private List<Pattern> patterns = new ArrayList<Pattern>();

    @Override
    public void setup(Context ctx) {
        String[] regexlist = ctx.getConfiguration().get("mapred.mapper.regex").split("\n");
        System.out.print("Found Regexes: " + regexlist.length);

        for (String item : regexlist) {
            if ("".equals(item)) continue; // don't add empty regexes
            try {
                patterns.add(Pattern.compile(item));
            } catch (Exception ex) {
                // not much to do...
            }
        }
    }

    @Override
    public void map(Text key, Text value, Context context)
    throws IOException {
        String text = value.toString();
        //System.out.println("Mapping : " + key.toString() + " value: " + value.toString());
        for (Pattern item : patterns) {
            Matcher matcher = item.matcher(text);
            while (matcher.find()) {
                try {
                    context.write(key, new Text(matcher.group()));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }
    }
}