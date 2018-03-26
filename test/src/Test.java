

/**
 * Created by dingxinbo on 26/03/18.
 */
public class Test {
    public static void main(String[] args) {
        String s ="zhangsan,18,192.168.36.22,8.8.8.8";
        String[] str = s.split(",");
        for(String i:str){
            System.out.println(i);
        }
    }
}




