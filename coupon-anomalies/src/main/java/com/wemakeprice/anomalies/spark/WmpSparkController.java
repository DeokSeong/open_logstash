package com.wemakeprice.anomalies.spark;

import com.wemakeprice.anomalies.common.AnomaliesCoupon;
import com.wemakeprice.anomalies.common.AnomaliesCouponConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.mutable.WrappedArray;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by open2jang@wemakeprice.com<br/>
 * Project Name: coupon-project<br/>
 * Package Name: com.wemakeprice.anomalies.spark<br/>
 * User: DeokSeongJang<br/>
 * Date: 2018. 3. 9. PM 4:51<br/>
 * Description:
 */
public class WmpSparkController implements Serializable {

    /** LOG Setting */
    private static Logger LOG = LogManager.getLogger(WmpSparkController.class);

    private static Configuration h_conf;

    private static SparkConf spark_conf;
    private static SparkSession spark_session;
    private static SQLContext spark_sql_context;

    public WmpSparkController(Configuration _conf){
        h_conf = _conf;
    }

    public void init(){

        if(spark_conf == null){

            String spark_job_name = "";
            if (h_conf.get(AnomaliesCouponConstant.SPARK_JOBNAME) != "") {
                spark_job_name = h_conf.get(AnomaliesCouponConstant.SPARK_JOBNAME);
            } else {
                LOG.error("설정파일에 '" + AnomaliesCouponConstant.SPARK_JOBNAME  +  "'가 설정되어 있지 않습니다.(../conf/wmp-config.xml 확인필요.)");
            }

            String spark_master = "";
            if (h_conf.get(AnomaliesCouponConstant.SPARK_MASTER) != "") {
                spark_master = h_conf.get(AnomaliesCouponConstant.SPARK_MASTER);
            } else {
                LOG.error("설정파일에 '" + AnomaliesCouponConstant.SPARK_MASTER  +  "'가 설정되어 있지 않습니다.(../conf/wmp-config.xml 확인필요.)");
            }

            String es_nodes = "";
            if (h_conf.get(AnomaliesCouponConstant.ES_NODES) != "") {
                es_nodes = h_conf.get(AnomaliesCouponConstant.ES_NODES);
            } else {
                LOG.error("설정파일에 '" + AnomaliesCouponConstant.ES_NODES  +  "'가 설정되어 있지 않습니다.(../conf/wmp-config.xml 확인필요.)");
            }

            String es_port = "";
            if (h_conf.get(AnomaliesCouponConstant.ES_PORT) != "") {
                es_port = h_conf.get(AnomaliesCouponConstant.ES_PORT);
            } else {
                LOG.error("설정파일에 '" + AnomaliesCouponConstant.ES_PORT  +  "'가 설정되어 있지 않습니다.(../conf/wmp-config.xml 확인필요.)");
            }

            String es_nodes_wan_only = "";
            if (h_conf.get(AnomaliesCouponConstant.ES_NODES_WAN_ONLY) != "") {
                es_nodes_wan_only = h_conf.get(AnomaliesCouponConstant.ES_NODES_WAN_ONLY);
            } else {
                LOG.error("설정파일에 '" + AnomaliesCouponConstant.ES_NODES_WAN_ONLY  +  "'가 설정되어 있지 않습니다.(../conf/wmp-config.xml 확인필요.)");
            }

            String es_index_auto_create = "";
            if (h_conf.get(AnomaliesCouponConstant.ES_INDES_AUTO_CREATE) != "") {
                es_index_auto_create = h_conf.get(AnomaliesCouponConstant.ES_INDES_AUTO_CREATE);
            } else {
                LOG.error("설정파일에 '" + AnomaliesCouponConstant.ES_INDES_AUTO_CREATE  +  "'가 설정되어 있지 않습니다.(../conf/wmp-config.xml 확인필요.)");
            }

            String es_net_http_auth_user = "";
            if (h_conf.get(AnomaliesCouponConstant.ES_NET_HTTP_AUTH_USER) != "") {
                es_net_http_auth_user = h_conf.get(AnomaliesCouponConstant.ES_NET_HTTP_AUTH_USER);
            } else {
                LOG.error("설정파일에 '" + AnomaliesCouponConstant.ES_NET_HTTP_AUTH_USER  +  "'가 설정되어 있지 않습니다.(../conf/wmp-config.xml 확인필요.)");
            }

            String es_net_http_auth_pass = "";
            if (h_conf.get(AnomaliesCouponConstant.ES_NET_HTTP_AUTH_PASS) != "") {
                es_net_http_auth_pass = h_conf.get(AnomaliesCouponConstant.ES_NET_HTTP_AUTH_PASS);
            } else {
                LOG.error("설정파일에 '" + AnomaliesCouponConstant.ES_NET_HTTP_AUTH_PASS  +  "'가 설정되어 있지 않습니다.(../conf/wmp-config.xml 확인필요.)");
            }

            String es_http_timeout = "";
            if (h_conf.get(AnomaliesCouponConstant.ES_HTTP_TIMEOUT) != "") {
                es_http_timeout = h_conf.get(AnomaliesCouponConstant.ES_HTTP_TIMEOUT);
            } else {
                LOG.error("설정파일에 '" + AnomaliesCouponConstant.ES_HTTP_TIMEOUT  +  "'가 설정되어 있지 않습니다.(../conf/wmp-config.xml 확인필요.)");
            }

            String es_index_type_name = "";
            if (h_conf.get(AnomaliesCouponConstant.ES_RESOURCE) != "") {
                es_index_type_name = h_conf.get(AnomaliesCouponConstant.ES_RESOURCE);
            } else {
                LOG.error("설정파일에 '" + AnomaliesCouponConstant.ES_RESOURCE  +  "'가 설정되어 있지 않습니다.(../conf/wmp-config.xml 확인필요.)");
            }

            LOG.info("es_net_http_auth_user: " + es_net_http_auth_user);
            LOG.info("es_net_http_auth_pass: "+ es_net_http_auth_pass);

            spark_conf = new SparkConf();

            spark_conf.setAppName(spark_job_name)
                    .setMaster(spark_master)
                    //.set("spark.dirver.bindAddress", "10.102.6.125")
                    .set(AnomaliesCouponConstant.ES_NODES, es_nodes)
                    .set(AnomaliesCouponConstant.ES_PORT, es_port)
                    .set(AnomaliesCouponConstant.ES_NODES_WAN_ONLY, es_nodes_wan_only)
                    .set(AnomaliesCouponConstant.ES_INDES_AUTO_CREATE, es_index_auto_create)
                    .set(AnomaliesCouponConstant.ES_NET_HTTP_AUTH_USER, es_net_http_auth_user)
                    .set(AnomaliesCouponConstant.ES_NET_HTTP_AUTH_PASS, es_net_http_auth_pass)
                    .set(AnomaliesCouponConstant.ES_HTTP_TIMEOUT, es_http_timeout)
                    .set(AnomaliesCouponConstant.ES_RESOURCE, es_index_type_name)
                    .set("es.read.field.as.array.include", "orders*")
                    .set("es.read.field.exclude", "payment_method")
                    .set("es.read.field.exclude", "platform");
            spark_session = org.apache.spark.sql.SparkSession.builder().config(spark_conf).getOrCreate();

            //java_spark_context = new JavaSparkContext(spark_conf);

            spark_sql_context = new org.apache.spark.sql.SQLContext(spark_session);

        }else{
            //java_spark_context = new JavaSparkContext(spark_conf);
            //spark_sql_context = new SQLContext(java_spark_context);
            spark_session = org.apache.spark.sql.SparkSession.builder().config(spark_conf).getOrCreate();
            spark_sql_context = new org.apache.spark.sql.SQLContext(spark_session);
        }
    }

    public JavaPairRDD<String, Map<String, Object>> makeRDDInput(){
        JavaPairRDD<String, Map<String, Object>> esRDD = null;

        String es_index_type_name = "";
        if (h_conf.get(AnomaliesCouponConstant.ES_RESOURCE) != "") {
            es_index_type_name = h_conf.get(AnomaliesCouponConstant.ES_RESOURCE);
        } else {
            LOG.error("설정파일에 '" + AnomaliesCouponConstant.ES_RESOURCE  +  "'가 설정되어 있지 않습니다.(../conf/wmp-config.xml 확인필요.)");
        }

        //esRDD = JavaEsSpark.esRDD(java_spark_context, es_index_type_name);

        return esRDD;
    }

    public Dataset<Row> convertRDDtoDataset(){

        /** 1. 수집되어 저장된 구매로그를 Elasticsearch에서 가져온다.*/
        Dataset<Row> input_set = JavaEsSparkSQL.esDF(spark_sql_context);

        /** Elasticsearch에 저장된 스키마 구조 print. */
        input_set.printSchema();

        /** 2. Spark에서 SQL문 사용을 위해 메모리에 임시 테이블 View 생성. */
        //ret_dataset.registerTempTable("coupon_info"); --> spark 2.* 사용하지 않는다.
        input_set.createOrReplaceTempView("coupon_info");

        /** 3. 필요한 필드 정의 후 select문을 실행하여 Dataset 생성. */
        Dataset<Row> payment_set = spark_sql_context.sql("SELECT payment_id, orders.deal_id, orders.sale_coupon_org, orders.order_time FROM coupon_info");
        payment_set.filter("payment_id='539180880'").show(1, false);
        //payment_set.show();

        /** 4. 모델링에 필요한 필드 추가 후 생성된 임시 Dataset 간 Join Start. */
        Dataset<Row> tmp_coupon_set = payment_set.withColumn("sale_coupon_org_unique", explode(payment_set.col("sale_coupon_org")));
        //tmp_coupon_set.show(100);

        //Dataset<Row> tmp_time_set = payment_set.withColumn("order_time_unique", explode(payment_set.col("order_time")));
        //tmp_time_set.show(100);
/*

        List<String> column_list = new ArrayList<String>();
        column_list.add("payment_id");
        column_list.add("deal_id");
        column_list.add("sale_coupon_org");
        column_list.add("order_time");
        Seq<String> use_column = JavaConverters.asScalaIteratorConverter(column_list.iterator()).asScala().toSeq();

        Dataset<Row> join_set = tmp_coupon_set.join(tmp_time_set, use_column, "right_outer");
*/

        //join_set.show(100);
        /** 4. 모델링에 필요한 필드 추가 후 생성된 임시 Dataset 간 Join End. */

/*
        //Convert WrappedArray<WrappedArray<Long>> To List<Long>>.
        UDF1<WrappedArray<WrappedArray<Long>>, List<Long>> getValue = new UDF1<WrappedArray<WrappedArray<Long>>, List<Long>>() {
            @Override
            public List<Long> call(WrappedArray<WrappedArray<Long>> data) throws Exception {
                List<Long> list_ret = new ArrayList<Long>();
                for(int i = 0; i < data.size(); i++) {
                    list_ret.addAll(JavaConversions.seqAsJavaList(data.apply(i)));
                }
                return null;
            }
        };

        spark_sql_context.udf().register("getValue", getValue, DataTypes.createArrayType(DataTypes.LongType));
*/

        /** Lambda Function: Convert WrappedArray<Long> To Long. */
        UDF1<WrappedArray<Long>, Long> getValue = new UDF1<WrappedArray<Long>, Long>() {
            @Override
            public Long call(WrappedArray<Long> data) throws Exception {
                Long long_ret = 0L;
                long_ret = data.apply(0).longValue();
                return long_ret;
            }
        };

        spark_sql_context.udf().register("getValue", getValue, DataTypes.LongType);

        Dataset<Row> result_set  = tmp_coupon_set.select(col("*"), callUDF("getValue", col("sale_coupon_org_unique")).as("udf-value"));

        result_set.groupBy("udf-value")
                .agg(col("udf-value").alias("coupon_price"), count(lit(1)).as("count"))
                .filter(col("coupon_price").gt(0))
                .orderBy(col("coupon_price").desc())
                .show(1000);

        result_set.groupBy("payment_id")
                .pivot("udf-value")
                .count()
                .show(1000);
/*

        result_set.select(col("payment_id"), col("deal_id"), col("udf-value"))
                  .groupBy("payment_id")
                  .agg(org.apache.spark.sql.functions.sum("udf-value").alias("tmp"))
                  .filter("tmp > 0").show(1000);
*/

/*
        // Custom Type Class에 대한 처리 방법: Encoder.
        Encoder<AnomaliesCoupon> coupon_encoder = Encoders.bean(AnomaliesCoupon.class);
        Dataset<AnomaliesCoupon> result_set = join_set.as(coupon_encoder);

        result_set.groupBy("sale_coupon_org_unique").agg(collect_list("deal_id").as("test")).show(1000);

        LOG.info("schema: " + coupon_encoder.schema());
*/

        return result_set;
    }

    public void clean() throws Exception{

        spark_sql_context.clearCache();
    }
    /*
    public WmpSparkController(Configuration _conf) {

        if(spark_conf == null){

            spark_conf = new SparkConf();

            String spark_job_name = "";
            if (_conf.get(AnomaliesCouponConstant.SPARK_JOBNAME) != "") {
                spark_job_name = _conf.get(AnomaliesCouponConstant.SPARK_JOBNAME);
            } else {
                LOG.error("설정파일에 '" + AnomaliesCouponConstant.SPARK_JOBNAME  +  "'가 설정되어 있지 않습니다.(../conf/wmp-config.xml 확인필요.)");
            }

            String spark_master = "";
            if (_conf.get(AnomaliesCouponConstant.SPARK_MASTER) != "") {
                spark_master = _conf.get(AnomaliesCouponConstant.SPARK_MASTER);
            } else {
                LOG.error("설정파일에 '" + AnomaliesCouponConstant.SPARK_MASTER  +  "'가 설정되어 있지 않습니다.(../conf/wmp-config.xml 확인필요.)");
            }

            String es_nodes = "";
            if (_conf.get(AnomaliesCouponConstant.ES_NODES) != "") {
                es_nodes = _conf.get(AnomaliesCouponConstant.ES_NODES);
            } else {
                LOG.error("설정파일에 '" + AnomaliesCouponConstant.ES_NODES  +  "'가 설정되어 있지 않습니다.(../conf/wmp-config.xml 확인필요.)");
            }

            String es_port = "";
            if (_conf.get(AnomaliesCouponConstant.ES_PORT) != "") {
                es_port = _conf.get(AnomaliesCouponConstant.ES_PORT);
            } else {
                LOG.error("설정파일에 '" + AnomaliesCouponConstant.ES_PORT  +  "'가 설정되어 있지 않습니다.(../conf/wmp-config.xml 확인필요.)");
            }

            String es_nodes_wan_only = "";
            if (_conf.get(AnomaliesCouponConstant.ES_NODES_WAN_ONLY) != "") {
                es_nodes_wan_only = _conf.get(AnomaliesCouponConstant.ES_NODES_WAN_ONLY);
            } else {
                LOG.error("설정파일에 '" + AnomaliesCouponConstant.ES_NODES_WAN_ONLY  +  "'가 설정되어 있지 않습니다.(../conf/wmp-config.xml 확인필요.)");
            }

            String es_index_auto_create = "";
            if (_conf.get(AnomaliesCouponConstant.ES_INDES_AUTO_CREATE) != "") {
                es_index_auto_create = _conf.get(AnomaliesCouponConstant.ES_INDES_AUTO_CREATE);
            } else {
                LOG.error("설정파일에 '" + AnomaliesCouponConstant.ES_INDES_AUTO_CREATE  +  "'가 설정되어 있지 않습니다.(../conf/wmp-config.xml 확인필요.)");
            }

            String es_net_http_auth_user = "";
            if (_conf.get(AnomaliesCouponConstant.ES_NET_HTTP_AUTH_USER) != "") {
                es_net_http_auth_user = _conf.get(AnomaliesCouponConstant.ES_NET_HTTP_AUTH_USER);
            } else {
                LOG.error("설정파일에 '" + AnomaliesCouponConstant.ES_NET_HTTP_AUTH_USER  +  "'가 설정되어 있지 않습니다.(../conf/wmp-config.xml 확인필요.)");
            }

            String es_net_http_auth_pass = "";
            if (_conf.get(AnomaliesCouponConstant.ES_NET_HTTP_AUTH_PASS) != "") {
                es_net_http_auth_pass = _conf.get(AnomaliesCouponConstant.ES_NET_HTTP_AUTH_PASS);
            } else {
                LOG.error("설정파일에 '" + AnomaliesCouponConstant.ES_NET_HTTP_AUTH_PASS  +  "'가 설정되어 있지 않습니다.(../conf/wmp-config.xml 확인필요.)");
            }

            String es_http_timeout = "";
            if (_conf.get(AnomaliesCouponConstant.ES_HTTP_TIMEOUT) != "") {
                es_http_timeout = _conf.get(AnomaliesCouponConstant.ES_HTTP_TIMEOUT);
            } else {
                LOG.error("설정파일에 '" + AnomaliesCouponConstant.ES_HTTP_TIMEOUT  +  "'가 설정되어 있지 않습니다.(../conf/wmp-config.xml 확인필요.)");
            }

            LOG.info("es_net_http_auth_user: " + es_net_http_auth_user);
            LOG.info("es_net_http_auth_pass: "+ es_net_http_auth_pass);

            spark_conf.setAppName(spark_job_name)
                    .setMaster(spark_master)
                    .set("spark.dirver.bindAddress", "10.102.6.125")
                    .set(AnomaliesCouponConstant.ES_NODES, es_nodes)
                    .set(AnomaliesCouponConstant.ES_PORT, es_port)
                    .set(AnomaliesCouponConstant.ES_NODES_WAN_ONLY, es_nodes_wan_only)
                    .set(AnomaliesCouponConstant.ES_INDES_AUTO_CREATE, es_index_auto_create)
                    .set(AnomaliesCouponConstant.ES_NET_HTTP_AUTH_USER, es_net_http_auth_user)
                    .set(AnomaliesCouponConstant.ES_NET_HTTP_AUTH_PASS, es_net_http_auth_pass)
                    .set(AnomaliesCouponConstant.ES_HTTP_TIMEOUT, es_http_timeout);

            java_spark_context = new JavaSparkContext(spark_conf);

        }else{
            java_spark_context = new JavaSparkContext(spark_conf);
        }
    }

    public static synchronized JavaSparkContext getInstance(Configuration _conf){

        synchronized (WmpSparkController.class) {

            if(java_spark_context == null){
                new WmpSparkController(_conf);
            }

            return java_spark_context;
        }
    }
    */
}
