import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.netease.mobile.offline.util.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcMap;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;
import java.util.Map;
//import com.netease.recsys.urs.util.UrsEncoder;

/**
 * @author xiaoyijian
 * @date 2017年8月24日 下午3:26:24
 * 
 *       科技中心推送的用户推荐标签数据解析.
 */
public class UserRecomTagMR2 extends Configured implements Tool {
    private static Logger LOG = Logger.getLogger(UserRecomTagMR2.class);
    //public static String REC_USR_TAG_IN = "/user/portal/ODM/RECOMMEND/app-user-info";
    //public static String REC_USR_TAG_OUT = "/user/portal/BDM/RECOMMEND/BDM_RECOMMEND_USER_TAG_DAY";
    //public static String REC_USR_TAG_IN = "hdfs://192.168.56.101:9000/user/hadoop/input/";
    //public static String REC_USR_TAG_OUT = "hdfs://192.168.56.101:9000/user/hadoop/output/";
    public static String REC_USR_TAG_IN = "D:/bigdata/data4";
    public static String REC_USR_TAG_OUT = "D:/bigdata/output6";

    @SuppressWarnings("unchecked")
    public static class UserRecomTagMapper extends Mapper<LongWritable, Text, NullWritable, OrcStruct> {

        private final String colStructure = "struct<device_uuid:string,passport:string,ebInter:map<string,string>,"
                                             + "commerceInter:map<string,string>,newsInter:map<string,string>,"
                                             + "mappInter:map<string,string>,power:string,ebPower:string,moneyPower:string,"
                                             + "gamePower:string,conMoney:string,house:string,car:string,hasChild:string,marital:string,status:string>";
        private final String userColStruct2 = "struct<device_uuid:string,passport:string,status:string>";
        private final String errorColStruct = "struct<context:string>";

        private TypeDescription schema = TypeDescription.fromString(colStructure);
        private OrcStruct orcStruct = (OrcStruct) OrcStruct.createValue(schema);
        private TypeDescription schema2 = TypeDescription.fromString(userColStruct2);
        private OrcStruct orcStruct2 = (OrcStruct) OrcStruct.createValue(schema2);
        private TypeDescription errorSchema = TypeDescription.fromString(errorColStruct);
        private OrcStruct errorOrcStruct = (OrcStruct) OrcStruct.createValue(errorSchema);

        private MultipleOrcOutputFormat<OrcStruct> multipleOrcOutputFormat;
        private NullWritable hive_k = NullWritable.get();

        private Text device_uuid =  new Text();
        private Text passport = new Text();
        private OrcMap<Text, Text> ebInter = (OrcMap<Text, Text>) orcStruct.getFieldValue(2);
        private OrcMap<Text, Text> commerceInter = (OrcMap<Text, Text>) orcStruct.getFieldValue(3);
        private OrcMap<Text, Text> newsInter = (OrcMap<Text, Text>) orcStruct.getFieldValue(4);
        private OrcMap<Text, Text> mappInter = (OrcMap<Text, Text>) orcStruct.getFieldValue(5);
        private Text power = new Text();
        private Text ebPower = new Text();
        private Text moneyPower = new Text();
        private Text gamePower = new Text();
        private Text conMoney = new Text();
        private Text house = new Text();
        private Text car = new Text();
        private Text hasChild = new Text();
        private Text marital = new Text();
        private Text status = new Text();

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            super.setup(context);
            multipleOrcOutputFormat = new MultipleOrcOutputFormat<OrcStruct>(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] line = value.toString().split("\t", 2);
            try {
                parseUserKey(line[0]);
                JSONObject jo = JSON.parseObject(line[1]);
                
                jsonToOrcMap(ebInter, JSON.parseObject(jo.getString("ebInter")));
                jsonToOrcMap(commerceInter, JSON.parseObject(jo.getString("commerceInter")));
                jsonToOrcMap(newsInter, JSON.parseObject(jo.getString("newsInter")));
                jsonToOrcMap(mappInter, JSON.parseObject(jo.getString("mappInter")));

                power = getColumn(jo, "power");
                ebPower = getColumn(jo, "ebPower");
                moneyPower = getColumn(jo, "moneyPower");
                gamePower = getColumn(jo, "gamePower");
                conMoney = getColumn(jo, "conMoney");
                house = getColumn(jo, "house");
                car = getColumn(jo, "car");
                hasChild = getColumn(jo, "hasChild");
                marital = getColumn(jo, "marital");
                status = getColumn(jo, "status");

                orcStruct.setAllFields(device_uuid, passport, ebInter, commerceInter, newsInter, mappInter, 
                                       power, ebPower, moneyPower, gamePower, conMoney, house, car, hasChild, marital, status);
                orcStruct2.setAllFields(device_uuid, passport,status);
                //context.write(hive_k, orcStruct);

                if(status.equals(new Text("404"))){
                    multipleOrcOutputFormat.write(orcStruct,"notfound");
                }else {
                    multipleOrcOutputFormat.write(orcStruct2,"collect");
                }

                ebInter.clear();
                commerceInter.clear();
                newsInter.clear();
                mappInter.clear();

            } catch (Exception e) {
                errorOrcStruct.setAllFields(value);
                multipleOrcOutputFormat.write(errorOrcStruct,"errorlog");
                context.getCounter("UserRecomTagMR", "parseError").increment(1);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            multipleOrcOutputFormat.close();
        }
        
        /**
         * @param userKey
         * 
         * parse HZ device ID to Galaxy device_uuid format.
         */
        private void parseUserKey(String userKey) {
            String DEVICE = "1@"; // device_uuid or IMEI.
            String ACCOUNT = "2@"; // passport.

            if (userKey.startsWith(DEVICE)) {
                //this.device_uuid =  new Text(UrsEncoder.decrypt(userKey.substring(DEVICE.length())));
                this.device_uuid =  new Text(userKey.substring(DEVICE.length()));
                this.passport = new Text();
            } else if (userKey.startsWith(ACCOUNT)) {
                this.device_uuid =  new Text();
                this.passport = new Text(userKey.substring(DEVICE.length()));
                //this.passport = new Text(UrsEncoder.decrypt(userKey.substring(DEVICE.length())));
            } else {
                this.device_uuid =  new Text();
                this.passport = new Text();
            }
        }
        
        /**
         * @param jo
         * @param column
         * @return
         * 
         * get a hive column from JSON String. 
         */
        private Text getColumn(JSONObject jo, String column) {
            return jo.getString(column) != null ? new Text(jo.getString(column)) : new Text();
        }

        
    }

    /**
     * @param om
     * @param jo
     * 
     * parse JSON object to OrcMap.
     */
    private static void jsonToOrcMap(OrcMap<Text, Text> om, JSONObject jo) {
        if (jo == null || jo.size() <= 0) {
            return;
        } else {
            for (Map.Entry<String, Object> entry : jo.entrySet()) {
                om.put(new Text(entry.getKey()), new Text(entry.getValue().toString()));
            }
        }
    }
    
    @Override
    public int run(String[] args) throws Exception {

        int retCode = 1;
        if (args.length == 1 && (args[0].length() == 8 || args[0].length() == 10)) {
            if (args[0].length() == 10) {
                args[0] = args[0].substring(0, 8);
            }
            LOG.info("start parse " + args[0]);
            //System.setProperty("hadoop.home.dir","D:\\bigdata\\hadoop-2.6.5\\hadoop-2.6.5");
            //String inputPath = REC_USR_TAG_IN + Utils.getPartSuffixDirStdDate(args[0]);
            String inputPath = REC_USR_TAG_IN ;
            //String outputPath = REC_USR_TAG_OUT + Utils.getPartSuffixDirOfD(args[0]);
            String outputPath = REC_USR_TAG_OUT + "/20171211";
            Configuration conf = new Configuration();
            LOG.info("input path:" + inputPath);
            LOG.info("out path:" + outputPath);
//            conf.set("orc.mapred.output.schema", "struct<device_uuid:string,passport:string,ebInter:map<string,string>,"
//                                                 + "commerceInter:map<string,string>,newsInter:map<string,string>,mappInter:map<string,string>,"
//                                                 + "power:string,ebPower:string,moneyPower:string,gamePower:string,conMoney:string,house:string,"
//                                                 + "car:string,hasChild:string,marital:string,status:string>");
            //conf.addResource("D:\\bigdata\\hadoop-2.6.5\\hadoop-2.6.5\\etc\\core-site.xml");
            //conf.set("mapred.jop.tracker", "hdfs://192.168.56.101:9001");
            //conf.set("fs.defaultFS", "hdfs://192.168.56.101:9000");
            Job job = Job.getInstance(conf);

            try {
                job.setJarByClass(UserRecomTagMR2.class);
                job.setJobName("UserRecomTagMR  " + inputPath);

                FileSystem fs = FileSystem.get(conf);
                if (fs.exists(new Path(outputPath))) {
                    fs.delete(new Path(outputPath), true);
                }

                job.setInputFormatClass(CombineTextInputFormat.class);
                final Long MAP_SPLIT_SIZE = 3 * 1024 * 1024 * 1024L;
                job.getConfiguration().set("mapreduce.input.fileinputformat.split.maxsize", MAP_SPLIT_SIZE.toString());
                
                //job.setOutputFormatClass(OrcOutputFormat.class);
                job.setOutputFormatClass(MultipleOrcOutputFormat.class);

                job.setMapOutputKeyClass(NullWritable.class);
                job.setMapOutputValueClass(OrcStruct.class);

                job.setOutputKeyClass(NullWritable.class);
                job.setOutputValueClass(OrcStruct.class);

                job.setMapperClass(UserRecomTagMapper.class);
                job.setNumReduceTasks(0);

                Utils.addAllFileToJob(inputPath, fs, job);
                MultipleOrcOutputs.setCompressOutput(job, true);
                MultipleOrcOutputs.setOutputPath(job, new Path(outputPath));

                retCode = job.waitForCompletion(true) ? 0 : 1;
            } catch (ClassNotFoundException | IOException | InterruptedException e) {
                LOG.error("job failed with error:" + e);
                System.exit(1);
            }

        } else {
            LOG.info("Usage: UserRecomTagMR yyyyMMdd");
            System.exit(1);
        }

        return retCode;
    }


    public static void main(String[] args) throws Exception {
        // ToolRunner.run(new Configuration(), new UserRecomTagMR(), args);
        ToolRunner.run(new UserRecomTagMR2(), args);
    }
}
