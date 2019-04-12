package com.imooc.hadoop.mapreduce.youtube.utils;

//w6Pbyg_kcEk	DrRek	388	Music	343	19096	3.78	376	290	0I_5Kv7mZcE	UIKsHSvAH30	LM2a3qGsmOA	4irUGiPe-EA	kKJl_FJviKI	rQdTSlLMKAA	CAuY6H4xzxc	irYf7cmmDok	v_MCVw12iTQ	K3nt9P8XeIo	mI1N1YAFODk	FS3w77BA3Yk	7_ixeiPaf_o	xoTcHI2OcMg	TS63Ae5p43E	Zw5KcG-9n_k	HkmTQUvBCHQ	loczWpMNAK4	jlYF4hDaE7U	iDVe5rR7jOk
public class FormatUtils {
    public static String FormatData(String ori){
        StringBuilder sb = new StringBuilder();
        String[] splitsArray = ori.split("\t");
        if(splitsArray.length < 9){ return null; }
        splitsArray[3] = splitsArray[3].replaceAll(" ","");
        for(int i=0;i<splitsArray.length;i++){
            sb.append(splitsArray[i]);
            if(i<9){
                if( i != splitsArray.length-1){
                    sb.append("\t");
                }
            }else {
                if( i != splitsArray.length-1){
                    sb.append("&");
                }
            }
        }
        return sb.toString();
    }
}
