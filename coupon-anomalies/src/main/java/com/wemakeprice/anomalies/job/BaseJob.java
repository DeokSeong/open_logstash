package com.wemakeprice.anomalies.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

/**
 * Created by open2jang@wemakeprice.com<br/>
 * Project Name: coupon-project<br/>
 * Package Name: com.wemakeprice.anomalies.job<br/>
 * User: DeokSeongJang<br/>
 * Date: 2018. 3. 9. PM 3:06<br/>
 * Description:
 */
public class BaseJob extends Configured {

    public BaseJob(Configuration h_conf){
        super(h_conf);
    }
}
