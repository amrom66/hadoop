package hive.entity;

import com.google.inject.internal.util.$Nullable;

import java.util.List;

/**
 * 数据表封装
 */
public class Table {
    private String tbName;
    private List<Field> fields;
    private String comment;
    private String tblproperties;
    private String location;

    public Table() {
    }

    public Table(String tbName, List<Field> fields, String comment, String tblproperties, String location) {
        this.tbName = tbName;
        this.fields = fields;
        this.comment = comment;
        this.tblproperties = tblproperties;
        this.location = location;
    }

    public String getTbName() {
        return tbName;
    }

    public void setTbName(String tbName) {
        this.tbName = tbName;
    }

    public String getFields() {
        StringBuffer stringBuffer = new StringBuffer();
        for (int i=0; i< fields.size()-1; i++){
            stringBuffer.append(fields.get(i)).append(",") ;
        }
        stringBuffer.append(fields.get(fields.size()-1));
        return stringBuffer.toString();
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getTblproperties() {
        return tblproperties;
    }

    public void setTblproperties(String tblproperties) {
        this.tblproperties = tblproperties;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    @Override
    public String toString() {

        String str=this.getTbName() + "(" + this.getFields() + ")";
        if (this.comment!=null){
            str += " comment '" + this.comment+ "'";
        }
        if (this.tblproperties!=null){
            str += " tblproperties " + this.tblproperties;
        }
        if (location!=null){
            str += " location " + this.location;
        }
        return str;
    }
}
