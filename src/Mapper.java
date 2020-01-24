import java.io.*;

public class Mapper {

    int numOfMapper;
    int numOfReducer;


    public Mapper(int numOfMapper, int numOfReducer) {
        if (CheckInput.isValidNumber(String.valueOf(numOfMapper))) {
            this.numOfMapper = numOfMapper;
        }
        if (CheckInput.isValidNumber(String.valueOf(numOfReducer))) {
            this.numOfReducer = numOfReducer;
        }
    }

    public void runMappers() throws IOException, InterruptedException {

        String[] cmd = {  "./CreateMappers.sh", "1", String.valueOf(numOfMapper),String.valueOf(numOfReducer)};
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

    public static void createMappingFunction(String importCode,String mapperFunction) throws IOException {

        if (importCode==null||mapperFunction==null)
            return;

            BufferedWriter fileWriter = new BufferedWriter(new FileWriter("./Files/map/Mapper.java"));


            fileWriter.write(importCode+
                    "\n" +
                    "public class Mapper {\n" +
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
                    "        writer.append(message+\"  :\\t\\t\");\n" +
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
                    "public static void saveMessage(int mapperNum,String message) throws IOException {\n" +
                            "        BufferedWriter writer=new BufferedWriter(new FileWriter(\"/dirc/Output/MapperInfo/mapperInfo\"+mapperNum+\".txt\",true));\n" +
                            "        writer.append(message);\n" +
                            "        writer.newLine();\n" +
                            "        writer.close();\n" +
                            "    }\n"+

                    "    public static void main(String[] args) throws IOException {\n" +
                    "\n" +
                            "        long startTime = System.currentTimeMillis();\n" +
                            "        long startMemory = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();\n"+
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
                    "        long finishTime = System.currentTimeMillis();\n" +
                    "   double takenTime = (finishTime-startTime+0.0)/1000;\n" +
                            "        long finishMemory = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();\n" +
                            "        double takenMemory = (finishMemory-startMemory+0.0)/(1024*1024);\n" +
                    "\n" +
                    "        saveMessage(mapperNum,\"Total time taken:\\t\"+takenTime+\"s\");\n" +
                    "        saveMessage(mapperNum,\"Taken Memory is :\\t\" + takenMemory + \"MB\");\n"+
                    "\n" +
                    "    }\n" +
                    "\n" +
                    "\n" +
                    "}");


            fileWriter.close();
    }

}



