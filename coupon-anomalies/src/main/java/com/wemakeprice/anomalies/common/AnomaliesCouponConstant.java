package com.wemakeprice.anomalies.common;

/**
 * Created by open2jang@wemakeprice.com<br/>
 * Project Name: coupon-project<br/>
 * Package Name: com.wemakeprice.anomalies.common<br/>
 * User: DeokSeongJang<br/>
 * Date: 2018. 3. 9. PM 2:58<br/>
 * Description:
 */
public class AnomaliesCouponConstant {

    public AnomaliesCouponConstant() {
        // TODO Auto-generated constructor stub
    }

    /** Product Info. */
    public static final String PRODUCT_NAME = "coupon-anomalies";
    public static final String PRODUCT_VERSION = "1.0.0";
    public static final String PRODUCT_BUILD_DATE = "2018-03-09";

    /** Spark Setting Start.*/
    /** SPARK_JOBNAME. */
    public static final String SPARK_JOBNAME = "spark.job.name";
    /** Spark 실행 시 필요한 Properties.*/
    public static final String SPARK_MASTER = "spark.master";
    /** yarn.*/
    public static final String SPARK_EXECUTOR_INSTANCES = "spark.executor.instances";
    public static final String SPARK_EXECUTOR_CORES = "spark.executor.cores";
    public static final String SPARK_EXECUTOR_MEMORY = "spark.executor.memory";
    public static final String SPARK_LOCAL_DIR = "spark.local.dir";
    public static final String SPARK_YARN_WAITTIME = "spark.yarn.am.waitTime";
    /** Spark Setting End.*/

    /** Elasticsearch Setting Start.*/
    /** ES_CLUSTER_NAME */
    public static final String ES_CLUSTER_NAME = "es.cluster.name";
    /** DEFAULT_CLUSTER_NAME. */
    public static final String DEFAULT_CLUSTER_NAME = "wmp-cluster";
    /** ES_NODES */
    public static final String ES_NODES = "es.nodes";
    /** ES_PORT */
    public static final String ES_PORT = "es.port";
    /** ES_NODES_WAN_ONLY */
    public static final String ES_NODES_WAN_ONLY = "es.nodes.wan.only";
    /** ES_INDES_AUTO_CREATE */
    public static final String ES_INDES_AUTO_CREATE = "es.index.auto.create";
    /** ES_NET_HTTP_AUTH_USER */
    public static final String ES_NET_HTTP_AUTH_USER = "es.net.http.auth.user";
    /** ES_NET_HTTP_AUTH_PASS */
    public static final String ES_NET_HTTP_AUTH_PASS = "es.net.http.auth.pass";
    /** ES_HTTP_TIMEOUT */
    public static final String ES_HTTP_TIMEOUT = "es.http.timeout";
    public static final String DEFAULT_ES_HTTP_TIMEOUT = "5m";
    /** ES_MAPPING_ID */
    public static final String ES_MAPPING_ID = "es.mapping.id";
    /** ES_INDEX_TYPE_NAME */
    public static final String ES_RESOURCE = "es.resource";
    /** Elasticsearch Setting End.*/
}
