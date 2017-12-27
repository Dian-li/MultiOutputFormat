import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.orc.OrcFile;
import org.apache.orc.Writer;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

/**
 * @author lipeidian
 * @date 2017年12月27日
 * @version 0.2
 *
 * 多路输出ORCfile;ORCfile格式支持自定义
 *
 */
public class MultipleOrcOutputs<V extends Writable> extends FileOutputFormat<NullWritable, V> {
    private static final String EXTENSION = ".orc";

    private MultiOrcRecordWriter recordWriter;
    private TaskAttemptContext taskAttemptContext;
    private NullWritable nullWritable = NullWritable.get();

    public MultipleOrcOutputs(){
        super();
    }

    public MultipleOrcOutputs(Context context)throws IOException,InterruptedException {
        if(this.taskAttemptContext == null){
            this.taskAttemptContext = new TaskAttemptContextImpl(context.getConfiguration(),
                    context.getTaskAttemptID(),
                    new WrappedStatusReporter(context));
        }
        try{
            recordWriter = (MultiOrcRecordWriter) ((OutputFormat) ReflectionUtils.newInstance(context.getOutputFormatClass(), context.getConfiguration()))
                    .getRecordWriter(context);
        }catch (ClassNotFoundException e){
            throw new IOException(e);
        }
    }

    @Override
    public RecordWriter<NullWritable,V> getRecordWriter(TaskAttemptContext taskAttemptContext) {
        this.taskAttemptContext = taskAttemptContext;
        this.recordWriter = new MultiOrcRecordWriter<V>(taskAttemptContext);
        return recordWriter;
    }

    /**
     * 写文件函数,key默认为NullWritable
     * @param value    orcStruct
     * @param colStruct  原始ORCfile schema,必须和日志的解析结构相同
     * @param userStruct    自定义ORCfile schema
     * @param baseOutPutPath    输出文件名
     * @throws IOException
     * @throws InterruptedException
     */
    public void write(V value,String colStruct,String userStruct,String baseOutPutPath)throws IOException,InterruptedException{
        if(baseOutPutPath.equals(null) || baseOutPutPath.length() == 0){//默认为原始名字:part
            baseOutPutPath = "";
        }
        this.recordWriter.setDefineOrcStruct(new DefineOrcStruct(colStruct,userStruct,baseOutPutPath));
        this.recordWriter.write(nullWritable,value);
    }

    /**
     * 关闭RecordWriter
     * @throws IOException
     * @throws InterruptedException
     */
    public void close()throws IOException, InterruptedException{
        this.recordWriter.close(this.taskAttemptContext);
    }


    class MultiOrcRecordWriter<V extends Writable> extends RecordWriter<NullWritable, V> {

        private final TaskAttemptContext taskAttemptContext;

        private Writer writer;
        private Configuration conf;
        private DefineOrcStruct userOrcStruct;
        private TreeMap<String, RecordWriter<NullWritable, V>> recordWriters = new TreeMap<String, RecordWriter<NullWritable, V>>();

        public MultiOrcRecordWriter(TaskAttemptContext taskAttemptContext) {
            this.conf = taskAttemptContext.getConfiguration();
            this.taskAttemptContext = taskAttemptContext;
        }

        public void setDefineOrcStruct(DefineOrcStruct struct){
            this.userOrcStruct = struct;
        }

        @Override
        public void write(NullWritable nullWritable, V v) throws IOException, InterruptedException {

            RecordWriter<NullWritable, V> recordWriter;
            recordWriter = recordWriters.get(this.userOrcStruct.getOrcFilename());
            if(recordWriter == null){
                int[] index = this.userOrcStruct.generateOrcIndex();
                this.conf.set(BASE_OUTPUT_NAME,userOrcStruct.getOrcFilename());
                this.conf.set("orc.mapred.output.schema",this.userOrcStruct.getUserDefineOrcStruct());
                Path filename = getDefaultWorkFile(taskAttemptContext, EXTENSION);
                this.writer = OrcFile.createWriter(filename,
                        org.apache.orc.mapred.OrcOutputFormat.buildOptions(conf));
                recordWriter = new OrcStructMRWriter<>(writer,index);
                this.recordWriters.put(this.userOrcStruct.getOrcFilename(),recordWriter);
            }
            recordWriter.write(nullWritable,v);
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException,InterruptedException {
            Iterator<String> keys = this.recordWriters.keySet().iterator();
            while (keys.hasNext()) {
                RecordWriter<NullWritable, V> rw = this.recordWriters.get(keys.next());
                rw.close(taskAttemptContext);
            }
            this.recordWriters.clear();
        }

    }

    private static class WrappedStatusReporter extends StatusReporter {

        TaskAttemptContext context;

        public WrappedStatusReporter(TaskAttemptContext context) {
            this.context = context;
        }

        @Override
        public org.apache.hadoop.mapreduce.Counter getCounter(Enum<?> name) {
            return context.getCounter(name);
        }

        @Override
        public org.apache.hadoop.mapreduce.Counter getCounter(String group, String name) {
            return context.getCounter(group, name);
        }

        @Override
        public void progress() {
            context.progress();
        }

        @Override
        public float getProgress() {
            return context.getProgress();
        }

        @Override
        public void setStatus(String status) {
            context.setStatus(status);
        }
    }

}
