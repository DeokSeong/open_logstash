package com.wemakeprice.anomalies.exec;

import org.apache.hadoop.conf.Configuration;

/**
 * Created by open2jang@wemakeprice.com<br/>
 * Project Name: coupon-project<br/>
 * Package Name: com.wemakeprice.anomalies.exec<br/>
 * User: DeokSeongJang<br/>
 * Date: 2018. 3. 9. PM 4:32<br/>
 * Description:
 */
public interface IAnomaliesCouponJob {
    public IAnomaliesCouponJob getInstance(Configuration _conf, String _couponType);
    public void init();
    public boolean run();
    public void close() throws Exception;
}
