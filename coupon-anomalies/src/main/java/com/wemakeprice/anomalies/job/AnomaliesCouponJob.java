package com.wemakeprice.anomalies.job;

import com.wemakeprice.anomalies.common.AnomaliesCouponConstant;
import com.wemakeprice.anomalies.config.ConfigManager;
import com.wemakeprice.anomalies.spark.WmpSparkController;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Created by open2jang@wemakeprice.com<br/>
 * Project Name: coupon-project<br/>
 * Package Name: com.wemakeprice.anomalies.job<br/>
 * User: DeokSeongJang<br/>
 * Date: 2018. 3. 9. PM 3:03<br/>
 * Description:
 */
public class AnomaliesCouponJob extends BaseJob{

    /** LOG. */
    private static Logger LOG = LogManager.getLogger(AnomaliesCouponJob.class);


    public AnomaliesCouponJob(Configuration _conf){
        super(_conf);
    }

    public int run(Configuration _conf) throws Exception{
        int ret = 0;

        double div = 0;
        long start = 0, end = 0;
        start = System.currentTimeMillis();

/*

        IAnomaliesCouponJob job = AnomaliesCouponFactory.getInstance(_conf, "");
        job.init();
        if(job.run()){
            end = System.currentTimeMillis();
            div = ((double) (end - start) / 1000);
            LOG.info("[Processing Total Time]" + div);
        }else{
            ret = -1;
        }

        job.close();
*/
        WmpSparkController spark = new WmpSparkController(_conf);

        spark.init();

        LOG.info("Modelling Anomalies Coupon Start");

        //JavaPairRDD<String, Map<String, Object>> esRDD = spark.makeRDDInput();

        //LOG.info("Elasticsearch Read Count: " + esRDD.count());

        Dataset<Row> input_dataset = spark.convertRDDtoDataset();


        LOG.info("Modelling Anomalies Coupon End");

        spark.clean();

        return ret;
    }

    public static int doMain(String[] args) throws Exception{

        LOG.info("\t\t======================================================");
        LOG.info("\t\tPRODUCT_NAME\t\t=\t" + AnomaliesCouponConstant.PRODUCT_NAME);
        LOG.info("\t\tPRODUCT_VERSION\t\t=\t" + AnomaliesCouponConstant.PRODUCT_VERSION);
        LOG.info("\t\tPRODUCT_BUILD_DATE\t=\t" + AnomaliesCouponConstant.PRODUCT_BUILD_DATE);
        LOG.info("\t\t======================================================");

        ConfigManager cm_config = new ConfigManager();

        if (!cm_config.config(args)) {
            return -1;
        }

        Configuration h_conf = cm_config.getH_conf();
        AnomaliesCouponJob job = new AnomaliesCouponJob(h_conf);

        String spark_master;
        spark_master = h_conf.get(AnomaliesCouponConstant.SPARK_MASTER);

        LOG.info("spark_master: " + spark_master);

        return job.run(h_conf);
    }

    public static void main(String[] args) {
        long startFilteringTime = 0;
        long endLFilteringTime = 0;
        long elaspedFilteringTime = 0;

        try {
            /** 타임구간 체크 start.*/
            startFilteringTime = System.currentTimeMillis();

            //LOG.info("RESULT:" + doMain(args));
            if(doMain(args) != 0){
                System.exit(-1);
            }

            endLFilteringTime = System.currentTimeMillis();
            elaspedFilteringTime = endLFilteringTime - startFilteringTime;

            LOG.info("elasped Time(m) for AnomaliesCouponJob :" + elaspedFilteringTime/1000);

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(-1);
        }
    }

}
