package System;

import View.HomeScreen;
import javafx.scene.paint.Color;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;


public class MapReduceSystem {

    private static int numOfMapper;
    private static int numOfReducer;
    private static Path filePath;
    private static String mapperFunction;
    private static String reducerFunction;
    private static String importMapper;
    private static String importReducer;

    private static final int PORT_NUM =7777;
    private static ServerSocket socket;

    public MapReduceSystem(SystemBuilder systemBuilder) {
        numOfMapper=systemBuilder.numOfMapper;
        numOfReducer=systemBuilder.numOfReducer;
        filePath=systemBuilder.filePath;
        mapperFunction=systemBuilder.mapperFunction;
        importMapper=systemBuilder.importMapper;
        reducerFunction=systemBuilder.reducerFunction;
        importReducer=systemBuilder.importReducer;
    }


    private  void createDirectories(){
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

    private  void waitMappers(int numOfNode) throws IOException {
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

    private  void initialisation() throws IOException {
        socket=new ServerSocket(PORT_NUM);
        createDirectories();
        Splitter.splitTextFiles(numOfMapper,filePath);
        ReducerManager.createReducerCode(importReducer,reducerFunction);
        MappersManager.createMappingCode(importMapper,mapperFunction);

    }

    private  void mapping() throws IOException, InterruptedException {
        MappersManager mapperManager=MappersManager.createMapperManager(numOfMapper,numOfReducer);
        mapperManager.runMappers();
    }

    private  void reducing() throws IOException, InterruptedException {
        ReducerManager reducer=new ReducerManager(numOfReducer);
        reducer.runReducers();
    }

    private  void compiling() throws IOException, InterruptedException {
        Process p = Runtime.getRuntime().exec("./Compiling.sh");
        p.waitFor();

    }


    public void start() throws IOException, InterruptedException {

      //  takeInput(inputs);
        initialisation();
        compiling();
        StatusReporter.saveTime("Start Mapping ");

        mapping();

        waitMappers(numOfMapper);
        StatusReporter.saveTime("Finish Mapping");
        reducing();
        StatusReporter.saveTime("Finish Reducing");
        HomeScreen.setStatus("Finish", Color.GREEN);
        socket.close();

    }


    public static class SystemBuilder {
        private  int numOfMapper;
        private  int numOfReducer;
        private  Path filePath;
        private  String mapperFunction;
        private  String reducerFunction;
        private  String importMapper;
        private  String importReducer;

        public SystemBuilder(int numOfMapper,int numOfReducer) {
            this.numOfMapper=numOfMapper;
            this.numOfReducer=numOfReducer;
        }
        public SystemBuilder inputFile(String inputFile) {
            this.filePath=Paths.get(inputFile);
            return this;
        }
        public SystemBuilder mapperCode(String mapperFunction,String importMapper) {
            this.mapperFunction = mapperFunction;
            this.importMapper=importMapper;
            return this;
        }
        public SystemBuilder reducerCode(String reducerFunction,String importReducer) {
            this.reducerFunction = reducerFunction;
            this.importReducer=importReducer;
            return this;
        }

        public MapReduceSystem build() {
            MapReduceSystem system =  new MapReduceSystem(this);
            return system;
        }


    }

    @Override
    public String toString() {
        return "MapReduceSystem{" +
                "Input File:\t"+filePath+
                "\nNumber of Mapper:\t"+numOfMapper+
                "\nNumber of Reducer:\t"+numOfReducer+"\n";
    }
}
