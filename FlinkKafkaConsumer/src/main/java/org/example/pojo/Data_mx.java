package org.example.pojo;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.util.HashMap;

/**
 * 作为所有细节数据的pojo父类，复用了几个重要方法
 * 用以实现对数据的特定化操作
 */
public abstract class Data_mx {

    /**
     * 各个pojo类执行自己自有的特定的sql语句<br>
     * 例子为 INSERT INTO dm.dm_v_tr_shop_mx (appKey, appVersion, deviceId, phone_no) VALUES (?, ?, ?, ?)
     */
    private String sql;

    /**
     * 复用的为每个具体的pojo类进行赋值的方法
     * @param eventBody 传入的数据的eventBody部分，即data_pojo的eventBdy部分
     * @return 返回是否为非空数据
     */
    public boolean setDataMx(HashMap<String, String> eventBody) {
        try {
            // 建立各个子类特有的sql语句
            StringBuilder insert = new StringBuilder("INSERT INTO dm.dm_v_tr_");
            StringBuilder values = new StringBuilder("VALUES (");
            // 将pojo名字小写并传入
            insert.append(this.getClass().getName().toLowerCase()).append("(");
            // 反射获取当前类声明区
            Field[] fields = this.getClass().getDeclaredFields();
            int count = 0; // 计算当前数据属性值为null的数量
            for (Field field : fields) {
                field.setAccessible(true); // 对于私有变量权限的释放
                // 进行语句的填写
                insert.append(field.getName()).append(",");
                values.append("? ,");
                // 获取对应属性值
                String value = eventBody.get(field.getName());
                if (value == null) {
                    count++;
                    field.set(this, null);
                } else {
                    // 进行不同类型属性的特殊赋值
                    if (field.getType().getName().equals("BigDecimal")){
                        field.set(this, new BigDecimal(value));
                    }else field.set(this, value);
                }
            }
            // 清除最后添加的多余的逗号，并增加括号
            int lastIndexOfInsertPoint = insert.lastIndexOf(",");
            insert.replace(lastIndexOfInsertPoint - 1, lastIndexOfInsertPoint, ")");

            int lastIndexOfValuesPoint = insert.lastIndexOf(",");
            values.replace(lastIndexOfValuesPoint - 1, lastIndexOfValuesPoint, ")");

            this.sql = insert.toString() + values;
            // 当null属性值个数小于数据体大小减一时返回true
            return count < eventBody.size() - 1;
        }catch (Exception e){
            e.printStackTrace();
            System.err.println("Pojo setDataMx error!");
        }
        return false;
    }

    public String getSql(){
        return this.sql;
    }

    /**
     * 用于对于不同pojo对象进行特殊的设置
     * @param preparedStatement JDBC的prepareStatement，对写好的SQL中的占位符进行赋值
     */
    public void setPreparedStatement(PreparedStatement preparedStatement) {
        try {
            Field[] fields = this.getClass().getDeclaredFields();
            int count = 0;
            for (Field field: fields) {
                field.setAccessible(true);

                if (field.getType().getName().equals("BigDecimal")){
                    BigDecimal value = (BigDecimal) field.get(this.getClass());
                    preparedStatement.setBigDecimal(count, value);
                }else {
                    String value = (String) field.get(this.getClass());
                    preparedStatement.setString(count, value);
                }
                count++;
            }
        }catch (Exception e){
            e.printStackTrace();
            System.err.println("Pojo setPreparedStatement method error!");
        }
    }
}
