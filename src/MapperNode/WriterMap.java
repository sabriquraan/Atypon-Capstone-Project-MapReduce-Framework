package MapperNode;

import Common.Writer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Map;

public class WriterMap implements Writer {

    public static void write(Map<?,?> map, BufferedWriter output) throws IOException {
        map.keySet().forEach(k-> {
            try {
                output.write(k + ":" + map.get(k) + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        output.close();


    }


}
