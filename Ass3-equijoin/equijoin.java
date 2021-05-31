
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class equijoin
{
    public static class MapNode extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        private Text kjoin = new Text();
        private Text tuples = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String row = value.toString();
            if(row.equals("")) {
                return;
            }
            String line[] = value.toString().split(",");
            //sets key and value

            kjoin.set(line[1]);
            tuples.set(row);
            output.collect(kjoin, tuples);
        }
    }

    public static class ReduceNode extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {


               String read1="";
			   String read2="";
//Creating separate lists for separate relations
			   ArrayList<String>  table_1 = new ArrayList<String>();
			   ArrayList<String>  table_2 = new ArrayList<String>();

			   Text result = new Text();
			   String jtuple = "";

//Iterate over the list of values corresponding to a particular key to separate the records
            for (Iterator<Text> it = values; it.hasNext(); ) {
                Text value = it.next();

                String r = value.toString();
                String r_columns[] = r.split(",");

                if(read1.equals(""))
                {
                    read1=r_columns[0];
                }
                else if(!read1.equals("") && !read1.equals(r_columns[0]) && read2.equals(""))
                {
                    read2=r_columns[0];
                }
                //checks for the joining key and add it separately in a table

                if(r_columns[0].equals(read1)){
                      table_1.add(r);
                  }
                  else if(r_columns[0].equals(read2)){
                      table_2.add(r);
                  }
            }


            if(table_1.size() == 0 || table_2.size() ==0) {
                key.clear();
            } else {
                //Adds value of S to R
                for(int i=0; i<table_2.size(); i++){
                    for(int j=0;j<table_1.size();j++){
                        jtuple= table_2.get(i) + "," + table_1.get(j);
                        result.set(jtuple);
                        output.collect(new Text(""), result);
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf config = new JobConf(equijoin.class);
        config.setJobName("equijoin job");
        config.setOutputKeyClass(Text.class);
        config.setOutputValueClass(Text.class);
        config.setMapperClass(MapNode.class);
        config.setReducerClass(ReduceNode.class);
        FileInputFormat.setInputPaths(config,new Path(args[0]));
        FileOutputFormat.setOutputPath(config,new Path(args[1]));
        JobClient.runJob(config);
    }
}