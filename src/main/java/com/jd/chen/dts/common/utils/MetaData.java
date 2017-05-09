package com.jd.chen.dts.common.utils;

import java.util.List;

/**
 * Created by chenxiaolei3 on 2017/4/20.
 */
public class MetaData {
    private String dataBaseName;

    private String dataBaseVersion;

    private String tableName = "default_table";

    private List<Column> colInfo;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDataBaseName() {
        return dataBaseName;
    }

    public void setDataBaseName(String dataBaseName) {
        this.dataBaseName = dataBaseName;
    }

    public String getDataBaseVersion() {
        return this.dataBaseVersion;
    }

    public void setDataBaseVersion(String dataBaseVersion) {
        this.dataBaseVersion = dataBaseVersion;
    }

    public List<Column> getColInfo() {
        return colInfo;
    }

    public void setColInfo(List<Column> colInfo) {
        this.colInfo = colInfo;
    }

    public class Column {
        private boolean isText = false;

        private boolean isNum = false;

        private String colName;

        private String dataType;

        private boolean isPK;

        public String getDataType() {
            return dataType;
        }

        public String getColName() {
            return colName;
        }

        public void setDataType(String dataType) {
            this.dataType = dataType;
        }

        public boolean isPK() {
            return isPK;
        }

        public void setPK(boolean isPK) {
            this.isPK = isPK;
        }

        public boolean isText() {
            return isText;
        }

        public void setText(boolean isText) {
            this.isText = isText;
        }

        public boolean isNum() {
            return isNum;
        }

        public void setNum(boolean isNum) {
            this.isNum = isNum;
        }

        public void setColName(String name) {
            colName = name;
        }
    }

}
