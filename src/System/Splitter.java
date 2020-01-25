package System;

import Common.Checker;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class Splitter {

    private Splitter(){

        throw new AssertionError();
    }

    public  static void splitTextFiles(int numOfMapper,Path bigFile) throws IOException {


        if (!Checker.isValidNumber(String.valueOf(numOfMapper)))
            throw new IllegalArgumentException("Error in number of mappers\n");

        if (!Checker.isFileExist(String.valueOf(bigFile)))
            throw new IllegalArgumentException("Error in input file path\n");


        long fileCountLine=Files.lines(bigFile).count();
        long maxRows=fileCountLine/numOfMapper -1;
        int mapperNum=1;

        try(BufferedReader reader = Files.newBufferedReader(bigFile)){
            String line = null;
            int lineNum = 1;


            Path splitFile = Paths.get("./Files/split/"+mapperNum + "split.txt");
            BufferedWriter writer = Files.newBufferedWriter(splitFile, StandardOpenOption.CREATE);

            while ((line = reader.readLine()) != null) {

                if(lineNum > maxRows && mapperNum != numOfMapper){
                    writer.close();
                    lineNum = 1;
                    mapperNum++;
                    splitFile = Paths.get("./Files/split/"+mapperNum + "split.txt");
                    writer = Files.newBufferedWriter(splitFile, StandardOpenOption.CREATE);
                }


                writer.append(line);
                writer.newLine();
                lineNum++;
            }

            writer.close();
        }
    }


}
