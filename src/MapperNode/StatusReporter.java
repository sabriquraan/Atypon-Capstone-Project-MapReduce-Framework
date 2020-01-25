package MapperNode;

import Common.Writer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class StatusReporter implements Writer {

    public static void saveTime(int mapperNumber,String message) throws IOException {
        Date date = new Date();
        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String stringDate = sdf.format(date);
        BufferedWriter writer=new BufferedWriter(new FileWriter("/dirc/Output/MapperInfo/mapperInfo"+mapperNumber+".txt",true));
        writer.append(message).append(" :\t\t");
        writer.append(stringDate);
        writer.newLine();
        writer.close();
    }

    public static void saveMessage(int mapperNumber,String message) throws IOException {
        BufferedWriter writer=new BufferedWriter(new FileWriter("/dirc/Output/MapperInfo/mapperInfo"+mapperNumber+".txt",true));
        writer.append(message);
        writer.newLine();
        writer.close();
    }
}
