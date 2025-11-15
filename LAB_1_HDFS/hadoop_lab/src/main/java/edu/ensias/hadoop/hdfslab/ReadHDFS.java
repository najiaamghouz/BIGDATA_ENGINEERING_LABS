package edu.ensias.hadoop.hdfslab;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class ReadHDFS {

    public static void main(String[] args) throws IOException {

        if (args.length < 1) {
            System.out.println("Usage: ReadHDFS <filename>");
            System.exit(1);
        }

        String filename = args[0];

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path filePath = new Path("/user/root/input/" + filename);

        FSDataInputStream inStream = fs.open(filePath);
        BufferedReader br = new BufferedReader(new InputStreamReader(inStream));

        String line;
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }

        br.close();
        inStream.close();
        fs.close();
    }
}
