package View;

import System.StatusReporter;
import System.MapReduceSystem;
import Common.Checker;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.layout.Border;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.scene.paint.Paint;
import javafx.scene.text.Font;
import javafx.scene.text.TextAlignment;

import java.io.IOException;
import java.nio.file.Path;

import static java.lang.Thread.sleep;


public class HomeScreen  implements Screen {

    private static VBox numReducerAll;
    private static VBox numMapperAll;
    private  static VBox root;
    private  static HBox functions;
    private  static HBox inputFile;
    private static Label status;
    private static TextField numOfMappers;
    private static TextField numOfReducers;
    private static TextField inputFilePath;
    private static TextArea reducerFunction;
    private static TextArea mapperFunction;
    private static TextArea importReducerFunction;
    private static TextArea importMapperFunction;
    private static Button startButton;
    private static Scene scene;
    private static String []inputs;


    public void initialisation(){

        inputs=new String[7];

        Label inputFileLabel = new Label("Input File Path");
        inputFilePath = new TextField("/home/sabri/Documents/Project/input.txt");

        Label mapperFunctionLabel = new Label("Mapper function");
        Label reducerFunctionLabel = new Label("Reducer function");

        startButton = new Button("Start");

      //  mapperFunction = new TextArea(" public static Map<?,?> mapper(BufferedReader source) throws IOException {\n\n\n\n}");
       // reducerFunction = new TextArea("  public static Map<?,?> reducer(Map<String,List<String>> reducerInput) throws IOException {\n\n\n\n}");
        mapperFunction=new TextArea("    public static Map<?,?> mapper(BufferedReader source) throws IOException {\n" +
                "\n" +
                "        Pattern pattern = Pattern.compile(\"[a-zA-Z]+\");\n" +
                "        TreeMap<String,Integer> wordCount = new TreeMap<String,Integer>();\n" +
                "        Matcher matcher ;\n" +
                "        String str = source.readLine();\n" +
                "        while(str!=null){\n" +
                "            if(!str.equals(\"\")){\n" +
                "                matcher = pattern.matcher(str);\n" +
                "                while(matcher.find()){\n" +
                "                    String word = matcher.group();\n" +
                "                    if(!wordCount.containsKey(word))\n" +
                "                        wordCount.put(word,1);\n" +
                "                    else\n" +
                "                        wordCount.put(word,wordCount.get(word)+1);\n" +
                "                }\n" +
                "            }\n" +
                "            str = source.readLine();\n" +
                "        }\n" +
                "\n" +
                "        return wordCount;\n" +
                "\n" +
                "\n" +
                "    }");
        reducerFunction=new TextArea("public static Map<?,?> reducer(Map<String,List<String>> reducerInput) throws IOException {\n" +
                "\n" +
                "\n" +
                "        TreeMap<String, Integer> reducer = new TreeMap<>();\n" +
                "\n" +
                "        reducerInput.keySet().forEach(k -> {\n" +
                "            List<String> values=reducerInput.get(k);\n" +
                "            int sumOfValues=0;\n" +
                "            for (String value:values)\n" +
                "                sumOfValues+=Integer.parseInt(value);\n" +
                "\n" +
                "            reducer.put(k,sumOfValues);\n" +
                "\n" +
                "        });\n" +
                "        return reducer;\n" +
                "\n" +
                "    }");
        importMapperFunction=new TextArea("import java.io.*;\n" +
                "import java.net.Socket;\n" +
                "import java.nio.channels.FileLock;\n" +
                "import java.text.DateFormat;\n" +
                "import java.text.SimpleDateFormat;\n" +
                "import java.util.Date;\n" +
                "import java.util.Map;\n" +
                "import java.util.TreeMap;\n" +
                "import java.util.regex.Matcher;\n" +
                "import java.util.regex.Pattern;\n");
        importReducerFunction=new TextArea("import java.io.*;\n" +
                "import java.net.ServerSocket;\n" +
                "import java.net.Socket;\n" +
                "import java.net.SocketException;\n" +
                "import java.text.DateFormat;\n" +
                "import java.text.SimpleDateFormat;\n" +
                "import java.util.*;\n");

        Label numOfMapperLabel = new Label("Number of mappers");
        Label numOfReducerLabel = new Label("Number of reducers");
        numOfMappers = new TextField("2");
        numOfReducers = new TextField("2");


        status = new Label("IDEL");


        inputFile=new HBox(inputFileLabel,inputFilePath);
        VBox mapperAll = new VBox(mapperFunctionLabel, importMapperFunction, mapperFunction);
        VBox reducerAll = new VBox(reducerFunctionLabel, importReducerFunction, reducerFunction);
        functions = new HBox(mapperAll, reducerAll);


        numMapperAll = new VBox(numOfMapperLabel,numOfMappers);
        numReducerAll = new VBox(numOfReducerLabel,numOfReducers);
        HBox numOfNodes = new HBox(numMapperAll, numReducerAll);
        root = new VBox(inputFile,functions, numOfNodes, startButton,status);

    }

    public void setSize(){

        importReducerFunction.setPrefWidth(1000);
        importReducerFunction.setPrefHeight(500);

        importMapperFunction.setPrefWidth(1000);
        importMapperFunction.setPrefHeight(500);

        mapperFunction.setPrefWidth(750);
        reducerFunction.setPrefWidth(750);
        mapperFunction.setPrefHeight(750);
        reducerFunction.setPrefHeight(750);


        inputFilePath.setPrefWidth(1000);
        numOfReducers.setPrefWidth(1000);
        numOfMappers.setPrefWidth(1000);

        startButton.setMaxWidth(200);
        startButton.setMinHeight(50);
    }

    public void formatLayout(){

        inputFile.setSpacing(10);
        functions.setSpacing(10);

        numMapperAll.setPadding(new Insets(10));
        numReducerAll.setPadding(new Insets(10));


        startButton.setPadding(new Insets(15));
        startButton.setBorder(Border.EMPTY);

        status.setPadding(new Insets(10));
        status.setTextFill(Color.GREEN);
        status.setFont(Font.font(18));
        status.setTextAlignment(TextAlignment.RIGHT);


        root.setPadding(new Insets(10));
        root.setSpacing(10);

    }

    public HomeScreen() {

        initialisation();
        setSize();
        formatLayout();

        startButton.setOnAction(event -> {
            try {
                startButtonFunction();
            } catch (IOException | InterruptedException e) {
                System.out.println("Exception in Start Button.\n");
                status.setText("Exception in Start Button.\n");
                status.setTextFill(Color.RED);
                e.printStackTrace();
            }
        });
        scene = new Scene(root,1600,800);

    }

    public static void takeInput(){
        inputs[0]=inputFilePath.getText();
        inputs[1]=numOfMappers.getText();
        inputs[2]=numOfReducers.getText();
        inputs[3]=mapperFunction.getText();
        inputs[4]=importMapperFunction.getText();
        inputs[5]=reducerFunction.getText();
        inputs[6]=importReducerFunction.getText();
    }

    public static void checkInput() throws IOException, InterruptedException {

        if (!Checker.isFileExist(inputs[0])) {
            status.setText("InValid Path of file , Enter Correct Path of Input File");
            status.setTextFill(Color.RED);
        }

        if( !Checker.isValidNumber(inputs[1])) {
            if (status.getTextFill()==Color.BLUE)  {
            status.setText("InValid number of mappers , Enter Correct number of mappers");
            status.setTextFill(Color.RED);
            } else if (status.getTextFill()==Color.RED){
                status.setText("InValid Path of Input File and InValid number of mappers");
            }
        }

        if(!Checker.isValidNumber(inputs[2])) {
            if (status.getTextFill()==Color.BLUE) {
                status.setText("InValid number of reducers , Enter Correct number of reducers");
                status.setTextFill(Color.RED);
            } else if (status.getTextFill()==Color.RED) {
                status.setText("Error : check path of input file , number of mappers  and number of reducers");
            }
        }

       if (!Checker.isCorrectMapperFunction(inputs[4],inputs[3])) {
           if (status.getTextFill()==Color.BLUE) {
               status.setText("Syntax Error in Mapper , Enter Correct function of mapper");
               status.setTextFill(Color.RED);
           } else if (status.getTextFill()==Color.RED) {
               status.setText("Error : check path of input file , number of mappers  , number of reducers and mapper function");
           }
           }
       if (!Checker.isCorrectReducerFunction(inputs[6],inputs[5])) {
            if (status.getTextFill() == Color.BLUE) {
                status.setText("Syntax Error in Reducer , Enter Correct function of reducer");
                status.setTextFill(Color.RED);
            } else if (status.getTextFill() == Color.RED) {
                status.setText("Error : check path of input file , number of mappers  , number of reducers , mapper function and reducer function");

            }

        }


    }

    public static void clear() throws IOException, InterruptedException {
        Process process2=Runtime.getRuntime().exec("rm -r Output");
        process2.waitFor();
        Process process=Runtime.getRuntime().exec("mkdir Output");
        process.waitFor();
    }

    public static void startButtonFunction() throws IOException, InterruptedException {
        clear();
        StatusReporter.saveTime("Start Processing");
        long startTime = System.currentTimeMillis();
        long startMemory = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        setStatus("Working...",Color.BLUE);
        takeInput();
        checkInput();
        if (status.getTextFill()==Color.BLUE) {
            MapReduceSystem mapReduceSystem=new MapReduceSystem.SystemBuilder(Integer.parseInt(inputs[1]),Integer.parseInt(inputs[2]))
                    .mapperCode(inputs[3],inputs[4]).reducerCode(inputs[5],inputs[6]).inputFile(inputs[0]).build();
            mapReduceSystem.start();
        }



        long finishTime = System.currentTimeMillis();
        double takenTime = (finishTime-startTime+0.0)/1000;
        long finishMemory = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        double takenMemory = (finishMemory-startMemory+0.0)/(1024*1024);

        StatusReporter.saveTime("Finish Processing");
        StatusReporter.saveMessage("Total time taken:\t"+takenTime+"s");
        StatusReporter.saveMessage("Taken Memory is :\t" + takenMemory + "MB");


    }

    public static void setStatus(String text, Paint color){
        if (text==null || color==null)
            return;

        status.setTextFill(color);
        status.setText(text);

    }

    public Scene getScreen(){
        return scene;
    }


    }


