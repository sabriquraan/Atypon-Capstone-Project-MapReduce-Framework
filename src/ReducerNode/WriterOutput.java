package ReducerNode;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class WriterOutput {
    public static void write(Map<?,?> map) throws IOException {

        BufferedWriter output = new BufferedWriter(new FileWriter("/dirc/Output/output.txt", true));
        map.keySet().forEach(k -> {
            try {
                output.append(k + "," + map.get(k) + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        output.close();
    }
}
