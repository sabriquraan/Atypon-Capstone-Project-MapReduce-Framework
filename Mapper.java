import java.io.*;
import java.net.Socket;
import java.nio.channels.FileLock;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Mapper {


    public static void shuffling(int mapperNum,int numOfReducer) throws IOException {
        BufferedWriter writer = null;
        FileOutputStream out=null;
        int min = 64;
        int max = 123;
        double total_length = max - min;
        double subrange_length = total_length / (numOfReducer);
        double current_start = min;
        int[] ranges = new int[numOfReducer + 1];
        for (int i = 0; i <= numOfReducer; ++i) {
            ranges[i] = (int) current_start;
            current_start += subrange_length;
        }
        int reducerNum = 0;
        BufferedReader in = new BufferedReader(new FileReader("/dirc/Files/keys/"+mapperNum+"keys.txt"));
        String line =in.readLine();
        while(line!=null){
            int uni = line.charAt(0);
            for (int i = 0; i < numOfReducer; i++) {
                if (uni >= ranges[i] && uni < ranges[i + 1]) {
                    reducerNum = i + 1;
                    break;
                }
            }
            writer = new BufferedWriter(new FileWriter("/dirc/Files/reducer/"+reducerNum+"reducer.txt",true));
            out= new FileOutputStream("/dirc/Files/reducer/"+reducerNum+"reducer.txt",true);
            FileLock lock=out.getChannel().lock();
            writer.append(line+"\n");
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

    public static void saveTime(int mapperNum,String message) throws IOException {
        Date date = new Date();
        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String stringDate = sdf.format(date);
        BufferedWriter writer=new BufferedWriter(new FileWriter("/dirc/Output/MapperInfo/mapperInfo"+mapperNum+".txt",true));
        writer.append(message+"  :");
        writer.append(stringDate+"\n");
        writer.close();
    }

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

    public static void write(Map<?,?> map,BufferedWriter output) throws IOException {
        map.keySet().forEach(k-> {
            try {
                output.write(k + ":" + map.get(k) + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        output.close();


    }

    public static void sendSignal() throws IOException {
        Socket socket = new Socket("127.0.0.1", 7777);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        out.writeUTF("Exit");
        out.close();
        socket.close();
    }


    public static void main(String[] args) throws IOException {
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

        shuffling(mapperNum,numOfReducer);

        saveTime(mapperNum,"Finish Shuffling Keys");
        saveTime(mapperNum,"send Signal");

        sendSignal();

        saveTime(mapperNum,"finish mapper");

    }


}