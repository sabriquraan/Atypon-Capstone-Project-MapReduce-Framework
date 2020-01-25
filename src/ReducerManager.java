import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

public class ReducerManager {
    int numOfReducer;

    public ReducerManager(int numOfReducer) {
        if(Checker.isValidNumber(String.valueOf(numOfReducer))) {
            this.numOfReducer = numOfReducer;
        }
    }

    public void runReducers() throws IOException, InterruptedException {

        String[] cmd = {  "./CreateReducers.sh", "1", String.valueOf(numOfReducer)};
        Process p = Runtime.getRuntime().exec(cmd);
        p.waitFor();

        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        BufferedReader errorReader = new BufferedReader(new InputStreamReader(p.getErrorStream()));


        String line = "";
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }

        line = "";
        while ((line = errorReader.readLine()) != null) {
            System.out.println(line);
        }
    }


    public static void  createReducerFunction(String importCode,String reducerCode) throws IOException {

        if (importCode==null || reducerCode==null)
            return;

            FileWriter fileWriter = new FileWriter("./Files/map/Reducer.java");
            fileWriter.write(importCode+
                    "\n" +
                    "public class Reducer {\n" +
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
                    reducerCode+
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
                    "    }  " +


                    "        public static void saveTime(int reducerNum,String message) throws IOException {\n" +
                    "        Date date = new Date();\n" +
                    "        DateFormat sdf = new SimpleDateFormat(\"yyyy-MM-dd HH:mm:ss.SSS\");\n" +
                    "        String stringDate = sdf.format(date);\n" +
                    "        BufferedWriter writer=new BufferedWriter(new FileWriter(\"/dirc/Output/ReducerInfo/reducerInfo\"+reducerNum+\".txt\",true));\n" +
                    "        writer.append(message+\"  :\\t\\t\");\n" +
                    "        writer.append(stringDate+\"\\n\");\n" +
                    "        writer.close();\n" +
                    "    }\n" +

                            "\n" +
                            "public static void saveMessage(int reducerNum,String message) throws IOException {\n" +
                            "        BufferedWriter writer=new BufferedWriter(new FileWriter(\"/dirc/Output/ReducerInfo/reducerInfo\"+reducerNum+\".txt\",true));\n" +
                            "        writer.append(message);\n" +
                            "        writer.newLine();\n" +
                            "        writer.close();\n" +
                            "    }\n"+


                    "    public static void main(String[] args) throws IOException, InterruptedException {\n" +
                    "\n" +
                    "        long startTime = System.currentTimeMillis();\n" +
                    "        long startMemory = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();\n"+
                    "        int reducerNum= Integer.parseInt(args[0]);\n" +
                    "        numOfReducer= Integer.parseInt(args[1]);\n" +
                    "\n" +
                    "        saveTime(reducerNum,\"Reducer start:\");\n" +
                    "        Map<String,List<String>> inputReducer=prepareInput(reducerNum);\n" +
                    "        Map<?,?> reducer=reducer(inputReducer);\n" +
                    "        writeResult(reducer,reducerNum);\n" +
                    "        saveTime(reducerNum,\"Reducer Finish:\");\n" +
                    "        long finishTime = System.currentTimeMillis();\n" +
                    "   double takenTime = (finishTime-startTime+0.0)/1000;\n" +
                    "        long finishMemory = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();\n" +
                    "        double takenMemory = (finishMemory-startMemory+0.0)/(1024*1024);\n" +
                    "\n" +
                    "        saveMessage(reducerNum,\"Total time taken:\\t\"+takenTime+\"s\");\n" +
                    "        saveMessage(reducerNum,\"Taken Memory is :\\t\" + takenMemory + \"MB\");\n"+
                    "\n" +
                    "\n" +
                    "    }\n" +
                    "\n" +
                    "}\n");
            fileWriter.close();

        }



}


