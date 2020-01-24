import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Analyser {


    public static void saveTime(String message) throws IOException {
        Date date = new Date();
        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String stringDate = sdf.format(date);
        BufferedWriter writer=new BufferedWriter(new FileWriter("./Output/MapReduceSystem.txt",true));
        writer.append(message).append(" :\t\t");
        writer.append(stringDate);
        writer.newLine();
        writer.close();
    }

    public static void saveMessage(String message) throws IOException {
        BufferedWriter writer=new BufferedWriter(new FileWriter("./Output/MapReduceSystem.txt",true));
        writer.append(message);
        writer.newLine();
        writer.close();
    }
}
