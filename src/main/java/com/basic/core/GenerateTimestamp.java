package com.basic.core;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class GenerateTimestamp {

  public static void main(String[] args) throws IOException {
    BufferedReader bufferedReader = new BufferedReader(new FileReader("G:\\BiStream\\data\\S1.txt"));
    BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("G:\\BiStream\\data\\S2.txt"));

    String line;
    int i = 0;
    int ms = 1;
    while ((line = bufferedReader.readLine()) != null) {
      i++;
      line = line + " " + ms;
      bufferedWriter.write(line);
      bufferedWriter.newLine();
      if (i == 30) {
        ms++;
        i = 0;
      }
    }
    bufferedReader.close();
    bufferedWriter.close();
  }
}
