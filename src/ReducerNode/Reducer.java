package ReducerNode;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static ReducerNode.Preparator.prepareInput;
import static ReducerNode.StatusReporter.saveMessage;
import static ReducerNode.StatusReporter.saveTime;
import static ReducerNode.WriterOutput.write;

public class Reducer {

    private static ServerSocket socket;
    private static int numOfReducer;

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

        write(reducer);

        saveTime(reducerNum,"Reducer befoar send signal:");

        if (reducerNum!=numOfReducer)
            sendSignal(reducerNum);
        else clean();

        saveTime(reducerNum,"Reducer after send signal:");
    }

    public static void clean() throws InterruptedException, IOException {
        Process process = Runtime.getRuntime().exec("rm -r /dirc/Files");
        process.waitFor();
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        long startTime = System.currentTimeMillis();
        long startMemory = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        int reducerNum= Integer.parseInt(args[0]);
        numOfReducer= Integer.parseInt(args[1]);

        saveTime(reducerNum,"Reducer start:");

        Map<String,List<String>> inputReducer=prepareInput(reducerNum);

        Map<?,?> reducer=reducer(inputReducer);

        writeResult(reducer,reducerNum);


        saveTime(reducerNum,"Reducer Finish:");
        long finishTime = System.currentTimeMillis();
        double takenTime = (finishTime-startTime+0.0)/1000;
        long finishMemory = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        double takenMemory = (finishMemory-startMemory+0.0)/(1024*1024);

        saveMessage(reducerNum,"Total time taken:\t"+takenTime+"s");
        saveMessage(reducerNum,"Taken Memory is :\t" + takenMemory + "MB");


    }

}
