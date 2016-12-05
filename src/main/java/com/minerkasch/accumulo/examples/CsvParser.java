package com.minerkasch.accumulo.examples;


import com.google.common.io.Files;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.nio.charset.Charset;

public class CsvParser {

    public enum Headers {
        callDateTime, priority, district, description, callNumber, incidentLocation, location
    }

    public static void main(String[] args) throws IOException {

//        File csvData = new File("/Users/zradtka/Downloads/Calls_for_Service.csv");
        Reader in = new FileReader("/Users/zradtka/Downloads/Calls_for_Service.csv");
        Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);
//        CSVParser parser = CSVParser.parse(csvData, Charset.defaultCharset(), CSVFormat.RFC4180);

        for (CSVRecord record : records) {
//            System.out.println(csvRecord.toString());
            String location = record.get("location");
            System.out.println(record);
        }

    }


}
