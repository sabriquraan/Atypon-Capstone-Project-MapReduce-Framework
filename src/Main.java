import javafx.scene.paint.Color;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;


public class Main {

    private static int numOfMapper;
    private static int numOfReducer;
    private static Path filePath;
    private static String mapperFunction;
    private static String reducerFunction;
    private static String importMapper;
    private static String importReducer;

    private static final int PORT_NUM =7777;
    private static ServerSocket socket;


    public static void createDirectories(){
        if( !new File("./Output").exists())
            new File("./Output").mkdir();
        if( !new File("./Output/MapperInfo").exists())
            new File("./Output/MapperInfo").mkdir();
        if( !new File("./Output/ReducerInfo").exists())
            new File("./Output/ReducerInfo").mkdir();
        if( !new File("./Files").exists())
            new File("Files").mkdir();
        if( !new File("./Files/map").exists())
            new File("./Files/map").mkdir();
        if( !new File("./Files/split").exists())
            new File("./Files/split").mkdir();
        if( !new File("./Files/keys").exists())
            new File("./Files/keys").mkdir();
        if( !new File("./Files/reducer").exists())
            new File("./Files/reducer").mkdir();

    }

    public static void waitMappers(int numOfNode) throws IOException {
        if (numOfNode <=0)
            throw new ArithmeticException("Error in number of nodes");

        String line = "";
        int count=1;
        while (count!=numOfNode ) {
            Socket client=socket.accept();
            DataInputStream in = new DataInputStream(new BufferedInputStream(client.getInputStream()));
            if (line.equals("Exit")) {
                count++;
            }
            line = in.readUTF();
        }
    }



    public static void initialisation() throws IOException, InterruptedException {
        socket=new ServerSocket(PORT_NUM);
        createDirectories();
        Splitting.splitTextFiles(numOfMapper,filePath);
        Reducer.createReducerFunction(importReducer,reducerFunction);
        Mapper.createMappingFunction(importMapper,mapperFunction);
    }

    public static void mapping() throws IOException, InterruptedException {
        Mapper mapper=new Mapper(numOfMapper,numOfReducer);
        mapper.runMappers();
    }

    public static void reducing() throws IOException, InterruptedException {
        Reducer reducer=new Reducer(numOfReducer);
        reducer.runReducers();
    }
    public static void compiling() throws IOException, InterruptedException {
        Process p = Runtime.getRuntime().exec("./Compiling.sh");
        p.waitFor();

    }
    public static void takeInput(String [] inputs){
        for (String input:inputs)
            if (input==null) {
                throw new IllegalArgumentException("Error in inputs\n");
            }
            filePath=Paths.get(inputs[0]);

            try {
                numOfMapper=Integer.parseInt(inputs[1]);
                numOfReducer=Integer.parseInt(inputs[2]);
            } catch (NumberFormatException e) {
                throw new NumberFormatException("Error in number of mappers or reduces\n");
            }
            mapperFunction=inputs[3];
            importMapper=inputs[4];
            reducerFunction=inputs[5];
            importReducer=inputs[6];


    }


    public static void start(String [] inputs) throws IOException, InterruptedException {

        takeInput(inputs);
        initialisation();
        compiling();
        Analyser.saveTime("Start Mapping ");

        mapping();

        waitMappers(numOfMapper);
        Analyser.saveTime("Finish Mapping");
        reducing();
        Analyser.saveTime("Finish Reducing");
        HomeScreen.setStatus("Finish", Color.GREEN);
        socket=null;

    }
}
