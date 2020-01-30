package System;

import java.io.File;
import java.nio.file.Path;

public class InputFile {

  File inputFile;
  Path location;

  public Path getLocation(){
    return location;

  }

  private long getSize(){

    return inputFile.length();
  }


  private  String getFileSizeKiloBytes() {
    return (double) inputFile.length() / 1024 + "  kb";
  }


}
