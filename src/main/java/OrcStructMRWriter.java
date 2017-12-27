import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.mapred.OrcKey;
import org.apache.orc.mapred.OrcMapredRecordWriter;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;

import java.io.IOException;

public class OrcStructMRWriter <V extends Writable>
        extends RecordWriter<NullWritable, V> {
    private final Writer writer;
    private final VectorizedRowBatch batch;
    private final TypeDescription schema;
    private final boolean isTopStruct;
    private final int[] orcStructIndex;

    public OrcStructMRWriter(Writer writer,int[] orcIndex){
        this.writer = writer;
        schema = writer.getSchema();
        this.batch = schema.createRowBatch();
        isTopStruct = schema.getCategory() == TypeDescription.Category.STRUCT;
        orcStructIndex = new int[orcIndex.length];
        for(int i=0;i<orcIndex.length;i++){
            orcStructIndex[i] = orcIndex[i];
        }
    }

    @Override
    public void write(NullWritable nullWritable, V v) throws IOException {
        // if the batch is full, write it out.
        if (batch.size == batch.getMaxSize()) {
            writer.addRowBatch(batch);
            batch.reset();
        }

        // add the new row
        int row = batch.size++;
        // skip over the OrcKey or OrcValue
        if (v instanceof OrcKey) {
            v = (V)((OrcKey) v).key;
        } else if (v instanceof OrcValue) {
            v = (V)((OrcValue) v).value;
        }
        if (isTopStruct) {
            for(int f=0; f < schema.getChildren().size(); ++f) {
                OrcMapredRecordWriter.setColumn(schema.getChildren().get(f),
                        batch.cols[f], row, ((OrcStruct) v).getFieldValue(orcStructIndex[f]));
            }
        } else {
            OrcMapredRecordWriter.setColumn(schema, batch.cols[0], row, v);
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException {
        if (batch.size != 0) {
            writer.addRowBatch(batch);
        }
        writer.close();
    }
}
