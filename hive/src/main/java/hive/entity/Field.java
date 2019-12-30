package hive.entity;

/**
 * 数据行
 */
public class Field {
    private String parmName;    //字段名称
    private String type;        //类型
    private String comment;     //描述

    public Field() {
    }

    public Field(String parmName, String type, String comment) {
        this.parmName = parmName;
        this.type = type;
        this.comment = comment;
    }

    public String getParmName() {
        return parmName;
    }

    public void setParmName(String parmName) {
        this.parmName = parmName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public String toString() {
        return parmName+ " " + type + " comment " + comment;
    }
}
