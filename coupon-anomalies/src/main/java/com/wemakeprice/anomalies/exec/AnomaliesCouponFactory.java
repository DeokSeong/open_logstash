package com.wemakeprice.anomalies.exec;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Created by open2jang@wemakeprice.com<br/>
 * Project Name: coupon-project<br/>
 * Package Name: com.wemakeprice.anomalies.exec<br/>
 * User: DeokSeongJang<br/>
 * Date: 2018. 3. 9. PM 5:30<br/>
 * Description:
 */
public class AnomaliesCouponFactory {
    /** LOG Setting. */
    private static Logger LOG = LogManager.getLogger(AnomaliesCouponFactory.class);

    /** Job Object. */
    protected static IAnomaliesCouponJob coupon_job;

    public AnomaliesCouponFactory() {
    }

    public static IAnomaliesCouponJob getInstance(Configuration _conf, String couponType) {

        //if(couponType.equalsIgnoreCase("I")){
            coupon_job = new ModelAnomaliesCoupon(_conf);
        //}

        return coupon_job;
    }
}
