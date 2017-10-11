package shengdong.hadoop.MapReduce.SVMlibTestDemo;

import java.io.IOException;

/**
 * Created by ShengdongSHI on 2017/4/6.
 */
public class SvmPredictMain {

    public static void main(String[] args){

        if(args.length < 3){
            System.out.println("----usage : <input predict file> <model file> <output path> ---");
        }

        SvmModelPredict predict = new SvmModelPredict(args[0],args[1],args[2]);
        try {
            predict.getModel();
            predict.predict();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
