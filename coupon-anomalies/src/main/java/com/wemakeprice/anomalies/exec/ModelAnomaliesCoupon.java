package com.wemakeprice.anomalies.exec;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Created by open2jang@wemakeprice.com<br/>
 * Project Name: coupon-project<br/>
 * Package Name: com.wemakeprice.anomalies.exec<br/>
 * User: DeokSeongJang<br/>
 * Date: 2018. 3. 9. PM 5:35<br/>
 * Description:
 */
public class ModelAnomaliesCoupon extends AnomaliesCouponJob {
    private static Logger LOG = LogManager.getLogger(ModelAnomaliesCoupon.class);

    public ModelAnomaliesCoupon(Configuration _conf) {
        super(_conf);
    }

    public boolean run() {

        LOG.info("Modelling Anomalies Coupon Start");

        //JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(java_spark_context, "wmp_coupon_alert/doc");

        //LOG.info("Elasticsearch Read Count: " + esRDD.count());


        LOG.info("Modelling Anomalies Coupon End");

        return true;
    }

}
