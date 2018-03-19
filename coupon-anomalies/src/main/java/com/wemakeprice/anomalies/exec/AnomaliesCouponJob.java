package com.wemakeprice.anomalies.exec;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;


/**
 * Created by open2jang@wemakeprice.com<br/>
 * Project Name: coupon-project<br/>
 * Package Name: com.wemakeprice.anomalies.exec<br/>
 * User: DeokSeongJang<br/>
 * Date: 2018. 3. 9. PM 4:36<br/>
 * Description:
 */
public abstract class AnomaliesCouponJob implements IAnomaliesCouponJob{

    /** LOG Setting. */
    private static Logger LOG = LogManager.getLogger(AnomaliesCouponJob.class);

    /** 설정파일. */
    protected Configuration h_conf = null;

    /** Job Object. */
    protected IAnomaliesCouponJob coupon_job;

    public static JavaSparkContext java_spark_context = null;

    public AnomaliesCouponJob(Configuration _conf) {
        h_conf = _conf;
    }

    @Override
    public IAnomaliesCouponJob getInstance(Configuration _conf, String _couponType) {
        h_conf = _conf;
        return coupon_job;
    }

    @Override
    public void init() {
        /** Create Spark Instance. */
        //java_spark_context = WmpSparkController.getInstance(h_conf);
    }

    @Override
    public boolean run() {
        return false;
    }

    @Override
    public void close() throws Exception{
        /*
        if(java_spark_context != null){
            java_spark_context.stop();
            // wait for jetty & spark to prperly shutdown
            Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        }
        */
    }
}
