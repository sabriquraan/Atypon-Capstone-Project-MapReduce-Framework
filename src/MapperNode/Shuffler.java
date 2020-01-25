package MapperNode;

import java.io.*;
import java.nio.channels.FileLock;

public class Shuffler {

    public static void startShuffling(int mapperNum,int numOfReducer) throws IOException {
        BufferedWriter writer = null;
        FileOutputStream out=null;

        int reducerNum = 0;
        BufferedReader in = new BufferedReader(new FileReader("/dirc/Files/keys/"+mapperNum+"keys.txt"));
        String line =in.readLine();
        while(line!=null){

            reducerNum=Partitioner.getPartition(line,numOfReducer);

            writer = new BufferedWriter(new FileWriter("/dirc/Files/reducer/"+reducerNum+"reducer.txt",true));
            out= new FileOutputStream("/dirc/Files/reducer/"+reducerNum+"reducer.txt",true);

            FileLock lock=out.getChannel().lock();
            writer.append(line).append("\n");
            writer.flush();
            writer.close();
            lock.release();
            out.close();
            line = in.readLine();
        }

        out.close();
        writer.close();
        in.close();

    }
}
