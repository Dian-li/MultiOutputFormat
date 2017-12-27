import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;

public class DefineOrcStruct {

    private int[] orcIndex;

    private String orcFilename;

    private String orcStruct;

    private String userDefineOrcStruct;

    public DefineOrcStruct(String orcStruct,String userDefineOrcStruct,String filename){
        this.orcStruct = orcStruct;
        this.userDefineOrcStruct = userDefineOrcStruct;
        this.orcFilename = filename;
    }

    public int[] generateOrcIndex(){
        TypeDescription hostTypeDes = TypeDescription.fromString(orcStruct);
        TypeDescription userTypeDes = TypeDescription.fromString(userDefineOrcStruct);
        orcIndex = new int[userTypeDes.getChildren().size()];
        int i=0,j=0;
        while(i<hostTypeDes.getFieldNames().size() && j<userTypeDes.getFieldNames().size()){
            if(hostTypeDes.getFieldNames().get(i).equals(userTypeDes.getFieldNames().get(j))){
                orcIndex[j] = i;
                j++;
            }
            i++;
        }
        return orcIndex;
    }

    public String getOrcFilename() {
        return orcFilename;
    }

    public void setOrcFilename(String orcFilename) {
        this.orcFilename = orcFilename;
    }



    public String getOrcStruct() {
        return orcStruct;
    }

    public void setOrcStruct(String orcStruct) {
        this.orcStruct = orcStruct;
    }

    public String getUserDefineOrcStruct() {
        return userDefineOrcStruct;
    }

    public void setUserDefineOrcStruct(String userDefineOrcStruct) {
        this.userDefineOrcStruct = userDefineOrcStruct;
    }


}
