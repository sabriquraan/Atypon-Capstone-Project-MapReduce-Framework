package Common;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class Checker {

    public static boolean isValidNumber(String text){
        if (text==null)
        return false;
        try {
            int number = Integer.parseInt(text);
            if (number<= 1)
                return false;
        } catch (NumberFormatException e) {
            return false;
        }


        return true;
    }

    public static boolean isFileExist(String text){
        if (text==null)
            return false;
        File tempFile = new File(text);
        boolean exists = tempFile.exists();
        return exists;
    }


    public static boolean isCorrectMapper(String importCode,String mapperFunction) throws IOException, InterruptedException {
        if (importCode==null || mapperFunction==null)
            return false;
        BufferedWriter writer=new BufferedWriter(new FileWriter("./test.java"));

        writer.write(importCode+
                "\n" +
                "public class test {\n" +
                "\n" +
                "\n" +
                "    public static void shuffling(int mapperNum,int numOfReducer) throws IOException {\n" +
                "        BufferedWriter writer = null;\n" +
                "        FileOutputStream out=null;\n" +
                "        int min = 64;\n" +
                "        int max = 123;\n" +
                "        double total_length = max - min;\n" +
                "        double subrange_length = total_length / (numOfReducer);\n" +
                "        double current_start = min;\n" +
                "        int[] ranges = new int[numOfReducer + 1];\n" +
                "        for (int i = 0; i <= numOfReducer; ++i) {\n" +
                "            ranges[i] = (int) current_start;\n" +
                "            current_start += subrange_length;\n" +
                "        }\n" +
                "        int reducerNum = 0;\n" +
                "        BufferedReader in = new BufferedReader(new FileReader(\"/dirc/Files/keys/\"+mapperNum+\"keys.txt\"));\n" +
                "        String line =in.readLine();\n" +
                "        while(line!=null){\n" +
                "            int uni = line.charAt(0);\n" +
                "            for (int i = 0; i < numOfReducer; i++) {\n" +
                "                if (uni >= ranges[i] && uni < ranges[i + 1]) {\n" +
                "                    reducerNum = i + 1;\n" +
                "                    break;\n" +
                "                }\n" +
                "            }\n" +
                "            writer = new BufferedWriter(new FileWriter(\"/dirc/Files/reducer/\"+reducerNum+\"reducer.txt\",true));\n" +
                "            out= new FileOutputStream(\"/dirc/Files/reducer/\"+reducerNum+\"reducer.txt\",true);\n" +
                "            FileLock lock=out.getChannel().lock();\n" +
                "            writer.append(line+\"\\n\");\n" +
                "            writer.flush();\n" +
                "            writer.close();\n" +
                "            lock.release();\n" +
                "            out.close();\n" +
                "            line = in.readLine();\n" +
                "        }\n" +
                "        out.close();\n" +
                "        writer.close();\n" +
                "        in.close();\n" +
                "\n" +
                "    }\n" +
                "\n" +
                "    public static void saveTime(int mapperNum,String message) throws IOException {\n" +
                "        Date date = new Date();\n" +
                "        DateFormat sdf = new SimpleDateFormat(\"yyyy-MM-dd HH:mm:ss.SSS\");\n" +
                "        String stringDate = sdf.format(date);\n" +
                "        BufferedWriter writer=new BufferedWriter(new FileWriter(\"/dirc/Output/MapperInfo/mapperInfo\"+mapperNum+\".txt\",true));\n" +
                "        writer.append(message+\"  :\");\n" +
                "        writer.append(stringDate+\"\\n\");\n" +
                "        writer.close();\n" +
                "    }\n" +
                "\n" +
                mapperFunction+
                "\n" +
                "    public static void write(Map<?,?> map,BufferedWriter output) throws IOException {\n" +
                "        map.keySet().forEach(k-> {\n" +
                "            try {\n" +
                "                output.write(k + \":\" + map.get(k) + \"\\n\");\n" +
                "            } catch (IOException e) {\n" +
                "                e.printStackTrace();\n" +
                "            }\n" +
                "        });\n" +
                "\n" +
                "        output.close();\n" +
                "\n" +
                "\n" +
                "    }\n" +
                "\n" +
                "    public static void sendSignal() throws IOException {\n" +
                "        Socket socket = new Socket(\"127.0.0.1\", 7777);\n" +
                "        DataOutputStream out = new DataOutputStream(socket.getOutputStream());\n" +
                "        out.writeUTF(\"Exit\");\n" +
                "        out.close();\n" +
                "        socket.close();\n" +
                "    }\n" +
                "\n" +
                "\n" +
                "    public static void main(String[] args) throws IOException {\n" +
                "        int mapperNum = Integer.parseInt(args[0]);\n" +
                "        int numOfReducer= Integer.parseInt(args[1]);\n" +
                "\n" +
                "        saveTime(mapperNum,\"Start Mapping\");\n" +
                "\n" +
                "        BufferedReader source = new BufferedReader(new FileReader(\"/dirc/Files/split/\"+ mapperNum +\"split.txt\"));\n" +
                "        BufferedWriter destination = new BufferedWriter(new FileWriter(\"/dirc/Files/keys/\"+ mapperNum +\"keys.txt\"));\n" +
                "        \n" +
                "        Map<?, ?> map=mapper(source);\n" +
                "        \n" +
                "        saveTime(mapperNum,\"Finish Mapping\");\n" +
                "        saveTime(mapperNum,\"Start Write Keys\");\n" +
                "        \n" +
                "        write(map,destination);\n" +
                "        \n" +
                "        saveTime(mapperNum,\"Finish Write Keys\");\n" +
                "        saveTime(mapperNum,\"Start Shuffling Keys\");\n" +
                "        \n" +
                "        shuffling(mapperNum,numOfReducer);\n" +
                "        \n" +
                "        saveTime(mapperNum,\"Finish Shuffling Keys\");\n" +
                "        saveTime(mapperNum,\"send Signal\");\n" +
                "        \n" +
                "        sendSignal();\n" +
                "        \n" +
                "        saveTime(mapperNum,\"finish mapper\");\n" +
                "\n" +
                "    }\n" +
                "\n" +
                "\n" +
                "}");
        writer.close();
        Process process=Runtime.getRuntime().exec("javac test.java");
        process.waitFor();
        if (new File("test.class").exists()){
            new File("test.java").delete();
            new File("test.class").delete();
            return true;
        } else {
            new File("test.java").delete();
            return false;
        }
    }

    public static boolean isCorrectReducer(String importCode,String reducerFunction) throws IOException, InterruptedException {
        if (importCode==null || reducerFunction==null)
            return false;
        BufferedWriter writer=new BufferedWriter(new FileWriter("./test2.java"));

        writer.write(importCode+
                "\n" +
                "public class test2 {\n" +
                "    \n" +
                "    private static ServerSocket socket;\n" +
                "    private static int numOfReducer;\n" +
                "\n" +
                "    public static Map<String,List<String>> prepareInput(int reducerNum) throws IOException {\n" +
                "\n" +
                "\n" +
                "        TreeMap<String, List<String>> reducer = new TreeMap<String, List<String>>();\n" +
                "        try (\n" +
                "                BufferedReader src = new BufferedReader(new FileReader(\"/dirc/Files/reducer/\"+reducerNum+\"reducer.txt\"));\n" +
                "        ) {\n" +
                "\n" +
                "            String str = src.readLine();\n" +
                "            while (str != null) {\n" +
                "                String[] s = str.split(\":\");\n" +
                "                reducer.computeIfAbsent(s[0], k -> new ArrayList<>()).add(s[1]);\n" +
                "                str = src.readLine();\n" +
                "            }\n" +
                "        }\n" +
                "\n" +
                "        return reducer;\n" +
                "    }\n" +
                "\n" +
                "\n" +
                "\n" +
                "\n" +
                reducerFunction+
                "\n" +
                "\n" +
                "    public static void waitFor(int waitFor) throws IOException {\n" +
                "        socket=new ServerSocket(8888);\n" +
                "        String line = \"\";\n" +
                "        while (!line.equals(String.valueOf(waitFor)) ) {\n" +
                "            Socket client=socket.accept();\n" +
                "            DataInputStream in = new DataInputStream(new BufferedInputStream(client.getInputStream()));\n" +
                "            line = in.readUTF();\n" +
                "        }\n" +
                "\n" +
                "    }\n" +
                "\n" +
                "    public static void sendSignal(int reducerNum) throws IOException {\n" +
                "        int dst=10+reducerNum+1;\n" +
                "        String ipDst=\"172.160.0.\"+dst;\n" +
                "        boolean flag=true;\n" +
                "        Socket socket = null;\n" +
                "\n" +
                "        while(flag){\n" +
                "            try {\n" +
                "                socket = new Socket(ipDst, 8888);\n" +
                "                flag=false;\n" +
                "\n" +
                "            } catch (SocketException e){\n" +
                "                continue;\n" +
                "            }\n" +
                "        }\n" +
                "        DataOutputStream out = new DataOutputStream(socket.getOutputStream());\n" +
                "        out.writeUTF(String.valueOf(reducerNum));\n" +
                "        out.close();\n" +
                "        socket.close();\n" +
                "    }\n" +
                "\n" +
                "\n" +
                "    public static void writeResult(Map<?,?> reducer,int reducerNum) throws IOException , InterruptedException{\n" +
                "\n" +
                "        boolean flag=true;\n" +
                "        while(flag){\n" +
                "            if (reducerNum==1) {\n" +
                "                saveTime(reducerNum,\"Reducer in while loop:\");\n" +
                "                flag=false;\n" +
                "            } else {\n" +
                "                saveTime(reducerNum,\"Reducer in while loop(wait):\");\n" +
                "                waitFor(reducerNum-1);\n" +
                "                flag=false;\n" +
                "                saveTime(reducerNum,\"Reducer in while loop(after wait):\");\n" +
                "\n" +
                "            }\n" +
                "        }\n" +
                "\n" +
                "        BufferedWriter dst = new BufferedWriter(new FileWriter(\"/dirc/Output/output.txt\", true));\n" +
                "        reducer.keySet().forEach(k -> {\n" +
                "            try {\n" +
                "                dst.append(k + \",\" + reducer.get(k) + \"\\n\");\n" +
                "            } catch (IOException e) {\n" +
                "                e.printStackTrace();\n" +
                "            }\n" +
                "        });\n" +
                "\n" +
                "        dst.close();\n" +
                "        saveTime(reducerNum,\"Reducer befoar send signal:\");\n" +
                "        if (reducerNum!=numOfReducer)\n" +
                "            sendSignal(reducerNum);\n" +
                "        else clean();\n" +
                "        saveTime(reducerNum,\"Reducer after send signal:\");\n" +
                "    }\n" +
                "    public static void clean() throws InterruptedException, IOException {\n" +
                "        Process process=Runtime.getRuntime().exec(\"rm -r /dirc/Files\");\n" +
                "        process.waitFor();\n" +
                "    }    public static void saveTime(int reducerNum,String message) throws IOException {\n" +
                "        Date date = new Date();\n" +
                "        DateFormat sdf = new SimpleDateFormat(\"yyyy-MM-dd HH:mm:ss.SSS\");\n" +
                "        String stringDate = sdf.format(date);\n" +
                "        BufferedWriter writer=new BufferedWriter(new FileWriter(\"/dirc/Output/ReducerInfo/reducerInfo\"+reducerNum+\".txt\",true));\n" +
                "        writer.append(message+\"  :\");\n" +
                "        writer.append(stringDate+\"\\n\");\n" +
                "        writer.close();\n" +
                "    }\n" +
                "    public static void main(String[] args) throws IOException, InterruptedException {\n" +
                "        int reducerNum= Integer.parseInt(args[0]);\n" +
                "        numOfReducer= Integer.parseInt(args[1]);\n" +
                "\n" +
                "        saveTime(reducerNum,\"Reducer start:\");\n" +
                "        Map<String,List<String>> inputReducer=prepareInput(reducerNum);\n" +
                "        Map<?,?> reducer=reducer(inputReducer);\n" +
                "        writeResult(reducer,reducerNum);\n" +
                "        saveTime(reducerNum,\"Reducer Finish:\");\n" +
                "\n" +
                "    }\n" +
                "\n" +
                "}\n");

        writer.close();
        Process process=Runtime.getRuntime().exec("javac test2.java");
        process.waitFor();
        if (new File("test2.class").exists()){
            new File("test2.java").delete();
            new File("test2.class").delete();
            return true;
        } else {
            new File("test2.java").delete();
            return false;
        }
    }

}
