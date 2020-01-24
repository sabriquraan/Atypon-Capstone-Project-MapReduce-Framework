import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class Reducer {

    private static ServerSocket socket;
    private static int numOfReducer;

    public static Map<String,List<String>> prepareInput(int reducerNum) throws IOException {


        TreeMap<String, List<String>> reducer = new TreeMap<String, List<String>>();
        try (
                BufferedReader src = new BufferedReader(new FileReader("/dirc/Files/reducer/"+reducerNum+"reducer.txt"));
        ) {

            String str = src.readLine();
            while (str != null) {
                String[] s = str.split(":");
                reducer.computeIfAbsent(s[0], k -> new ArrayList<>()).add(s[1]);
                str = src.readLine();
            }
        }

        return reducer;
    }




    public static Map<?,?> reducer(Map<String,List<String>> reducerInput) throws IOException {


        TreeMap<String, Integer> reducer = new TreeMap<>();

        reducerInput.keySet().forEach(k -> {
            List<String> values=reducerInput.get(k);
            int sumOfValues=0;
            for (String value:values)
                sumOfValues+=Integer.parseInt(value);

            reducer.put(k,sumOfValues);

        });
        return reducer;

    }


    public static void waitFor(int waitFor) throws IOException {
        socket=new ServerSocket(8888);
        String line = "";
        while (!line.equals(String.valueOf(waitFor)) ) {
            Socket client=socket.accept();
            DataInputStream in = new DataInputStream(new BufferedInputStream(client.getInputStream()));
            line = in.readUTF();
        }

    }

    public static void sendSignal(int reducerNum) throws IOException {
        int dst=10+reducerNum+1;
        String ipDst="172.160.0."+dst;
        boolean flag=true;
        Socket socket = null;

        while(flag){
            try {
                socket = new Socket(ipDst, 8888);
                flag=false;

            } catch (SocketException e){
                continue;
            }
        }
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        out.writeUTF(String.valueOf(reducerNum));
        out.close();
        socket.close();
    }


    public static void writeResult(Map<?,?> reducer,int reducerNum) throws IOException , InterruptedException{

        boolean flag=true;
        while(flag){
            if (reducerNum==1) {
                saveTime(reducerNum,"Reducer in while loop:");
                flag=false;
            } else {
                saveTime(reducerNum,"Reducer in while loop(wait):");
                waitFor(reducerNum-1);
                flag=false;
                saveTime(reducerNum,"Reducer in while loop(after wait):");

            }
        }

        BufferedWriter dst = new BufferedWriter(new FileWriter("/dirc/Output/output.txt", true));
        reducer.keySet().forEach(k -> {
            try {
                dst.append(k + "," + reducer.get(k) + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        dst.close();
        saveTime(reducerNum,"Reducer befoar send signal:");
        if (reducerNum!=numOfReducer)
            sendSignal(reducerNum);
        else clean();
        saveTime(reducerNum,"Reducer after send signal:");
    }
    public static void clean() throws InterruptedException, IOException {
        Process process=Runtime.getRuntime().exec("rm -r /dirc/Files");
        process.waitFor();
    }    public static void saveTime(int reducerNum,String message) throws IOException {
        Date date = new Date();
        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String stringDate = sdf.format(date);
        BufferedWriter writer=new BufferedWriter(new FileWriter("/dirc/Output/ReducerInfo/reducerInfo"+reducerNum+".txt",true));
        writer.append(message+"  :");
        writer.append(stringDate+"\n");
        writer.close();
    }
    public static void main(String[] args) throws IOException, InterruptedException {
        int reducerNum= Integer.parseInt(args[0]);
        numOfReducer= Integer.parseInt(args[1]);

        saveTime(reducerNum,"Reducer start:");
        Map<String,List<String>> inputReducer=prepareInput(reducerNum);
        Map<?,?> reducer=reducer(inputReducer);
        writeResult(reducer,reducerNum);
        saveTime(reducerNum,"Reducer Finish:");

    }

}
