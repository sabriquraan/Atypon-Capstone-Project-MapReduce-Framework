package MapperNode;

import java.io.*;
import java.net.Socket;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static MapperNode.StatusReporter.saveMessage;
import static MapperNode.StatusReporter.saveTime;
import static MapperNode.WriterMap.write;

public class MapperNode implements Mapper{

    public static Map<?,?> mapper(BufferedReader source) throws IOException {

        Pattern pattern = Pattern.compile("[a-zA-Z]+");
        TreeMap<String,Integer> wordCount = new TreeMap<String,Integer>();
        Matcher matcher ;
        String str = source.readLine();
        while(str!=null){
            if(!str.equals("")){
                matcher = pattern.matcher(str);
                while(matcher.find()){
                    String word = matcher.group();
                    if(!wordCount.containsKey(word))
                        wordCount.put(word,1);
                    else
                        wordCount.put(word,wordCount.get(word)+1);
                }
            }
            str = source.readLine();
        }

        return wordCount;

    }

    public static void sendSignal() throws IOException {
        Socket socket = new Socket("127.0.0.1", 7777);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        out.writeUTF("Exit");
        out.close();
        socket.close();
    }

    public static void main(String[] args) throws IOException {

        long startTime = System.currentTimeMillis();
        long startMemory = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        int mapperNum = Integer.parseInt(args[0]);
        int numOfReducer= Integer.parseInt(args[1]);

        saveTime(mapperNum,"Start Mapping");

        BufferedReader source = new BufferedReader(new FileReader("/dirc/Files/split/"+ mapperNum +"split.txt"));
        BufferedWriter destination = new BufferedWriter(new FileWriter("/dirc/Files/keys/"+ mapperNum +"keys.txt"));

        Map<?, ?> map=mapper(source);

        saveTime(mapperNum,"Finish Mapping");
        saveTime(mapperNum,"Start Write Keys");

        write(map,destination);

        saveTime(mapperNum,"Finish Write Keys");
        saveTime(mapperNum,"Start Shuffling Keys");

        Shuffler.startShuffling(mapperNum,numOfReducer);

        saveTime(mapperNum,"Finish Shuffling Keys");
        saveTime(mapperNum,"send Signal");

        sendSignal();

        saveTime(mapperNum,"finish mapper");
        long finishTime = System.currentTimeMillis();
        double takenTime = (finishTime-startTime+0.0)/1000;
        long finishMemory = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        double takenMemory = (finishMemory-startMemory+0.0)/(1024*1024);

        saveMessage(mapperNum,"Total time taken:\t"+takenTime+"s");
        saveMessage(mapperNum,"Taken Memory is :\t" + takenMemory + "MB");

    }


}