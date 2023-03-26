package org.example;

/**
 * 用于进行Data_pojo传来的数据为哪种类型数据判断的类
 */
public class Types {
   private final String[] types = {"contract", "djk", "dsf", "duebill", "etc", "grwy",
           "gzdf", "huanb", "huanx", "sa", "sbyb", "sdrq", "shop", "sjyh"};


    /**
     * 根据传入的eventType返回真实对应的type名<br>
     * 此处其实可以不必返回string，但是考虑到后续可能的类型翻译所以编写
     * @param type Data_pojo的eventType
     * @return 真实对应的pojo类型
     */
   public String getTypeString(String type){
       int index = -1;
       for (int i = 0; i < types.length; i++){
           if (type.equals(types[i])) {
               index = i;
               break;
           }
       }
       return types[index];
   }
}
