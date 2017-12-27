import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.orc.*;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcMapreduceRecordWriter;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

/**
 * @author lipeidian
 * @date 2017年12月27日
 * @version 0.1
 *
 * 多路输出ORCfile;ORCfile格式支持自定义
 *
 */
public class MultipleOrcOutputFormat<V extends Writable> extends FileOutputFormat<NullWritable, V> {
    private static final String EXTENSION = ".orc";

    private MultiOrcRecordWriter recordWriter;
    private TaskAttemptContext taskAttemptContext;
    private NullWritable nullWritable = NullWritable.get();

    public MultipleOrcOutputFormat(){
        super();
    }

    public MultipleOrcOutputFormat(Context context)throws IOException,InterruptedException {
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
     * @param baseOutPutPath    输出文件名
     * @throws IOException
     * @throws InterruptedException
     */
    public void write(V value,String baseOutPutPath)throws IOException,InterruptedException{
        if(baseOutPutPath.equals(null) || baseOutPutPath.length() == 0){//默认为原始名字:part
            baseOutPutPath = "";
        }
        this.recordWriter.setBaseOutputPath(baseOutPutPath);
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

    public static OrcFile.WriterOptions buildOptions(Configuration conf) {
        return OrcFile.writerOptions(conf)
                .version(OrcFile.Version.byName(OrcConf.WRITE_FORMAT.getString(conf)))
                .compress(CompressionKind.valueOf(OrcConf.COMPRESS.getString(conf)))
                .encodingStrategy(OrcFile.EncodingStrategy.valueOf
                        (OrcConf.ENCODING_STRATEGY.getString(conf)))
                .bloomFilterColumns(OrcConf.BLOOM_FILTER_COLUMNS.getString(conf))
                .bloomFilterFpp(OrcConf.BLOOM_FILTER_FPP.getDouble(conf))
                .blockSize(OrcConf.BLOCK_SIZE.getLong(conf))
                .blockPadding(OrcConf.BLOCK_PADDING.getBoolean(conf))
                .stripeSize(OrcConf.STRIPE_SIZE.getLong(conf))
                .rowIndexStride((int) OrcConf.ROW_INDEX_STRIDE.getLong(conf))
                .bufferSize((int) OrcConf.BUFFER_SIZE.getLong(conf))
                .paddingTolerance(OrcConf.BLOCK_PADDING_TOLERANCE.getDouble(conf));
    }

    class MultiOrcRecordWriter<V extends Writable> extends RecordWriter<NullWritable, V> {

        private final TaskAttemptContext taskAttemptContext;

        private Writer writer;
        private Configuration conf;
        private String baseOutputPath;
        private TreeMap<String, RecordWriter<NullWritable, V>> recordWriters = new TreeMap<String, RecordWriter<NullWritable, V>>();

        public MultiOrcRecordWriter(TaskAttemptContext taskAttemptContext) {
            this.conf = taskAttemptContext.getConfiguration();
            this.taskAttemptContext = taskAttemptContext;
        }

        public void setBaseOutputPath(String outPutName){
            this.baseOutputPath = outPutName;
        }

        @Override
        public void write(NullWritable nullWritable, V v) throws IOException, InterruptedException {

            RecordWriter<NullWritable, V> recordWriter;
            recordWriter = recordWriters.get(this.baseOutputPath);
            if(recordWriter == null){
                this.conf.set(BASE_OUTPUT_NAME,this.baseOutputPath);

                Path filename = getDefaultWorkFile(taskAttemptContext, EXTENSION);
                TypeDescription schema = ((OrcStruct)v).getSchema();
                OrcFile.WriterOptions writerOptions = MultipleOrcOutputFormat.buildOptions(conf);
                writerOptions.setSchema(schema);
                this.writer = OrcFile.createWriter(filename,writerOptions);
                recordWriter = new OrcMapreduceRecordWriter<>(writer);
                this.recordWriters.put(this.baseOutputPath,recordWriter);
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
