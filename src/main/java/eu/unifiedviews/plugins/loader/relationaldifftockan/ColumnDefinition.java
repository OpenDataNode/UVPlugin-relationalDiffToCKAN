package eu.unifiedviews.plugins.loader.relationaldifftockan;

public class ColumnDefinition {

    private String columnName;

    private String columnTypeName;

    private int columnType;

    public ColumnDefinition(String columnName, String columnTypeName, int columnType) {
        this.columnName = columnName;
        this.columnTypeName = columnTypeName;
        this.columnType = columnType;
    }

    public String getColumnName() {
        return this.columnName;
    }

    public String getColumnTypeName() {
        return this.columnTypeName;
    }

    public int getColumnType() {
        return this.columnType;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ColumnDefinition)) {
            return false;
        }
        ColumnDefinition cd = (ColumnDefinition) o;
        if (this.columnName.equals(cd.getColumnName()) && this.columnType == cd.getColumnType()) {
            return true;
        } else {
            return false;
        }
    }

}
