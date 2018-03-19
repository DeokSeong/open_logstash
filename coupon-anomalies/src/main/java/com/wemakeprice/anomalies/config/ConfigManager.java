package com.wemakeprice.anomalies.config;

import com.wemakeprice.anomalies.common.AnomaliesCouponConstant;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

/**
 * Created by open2jang@wemakeprice.com<br/>
 * Project Name: couponproject<br/>
 * Package Name: com.wemakeprice.anomalies.config<br/>
 * User: DeokSeongJang<br/>
 * Date: 2018. 3. 9. PM 2:21<br/>
 * Description:
 */
public class ConfigManager {

    /** LOG Setting. */
    private static Logger LOG = LogManager.getLogger(ConfigManager.class);

    /** Conf in Hadoop Module. */
    private Configuration h_conf = null;

    /** Spark Job Name. */
    private String spark_jobname = "";

    public void setH_conf(Configuration h_conf) {
        this.h_conf = h_conf;
    }

    public Configuration getH_conf() {
        return h_conf;
    }

    /**
     * coupon-anomalies 모듈의 실행을 위한 옵션 정의.
     * @return
     */
    private Options makeOptions(){

        Options options = new Options();

        /** 설정파일이름: wmp-config.xml, class path를 설정하므로 전체 경로가 아닌 설정파일 이름. */
        Option configOption = new Option("c", "config", true, "config file or directory");

        /** true 이면 필수항목. */
        configOption.setRequired(true);

        options.addOption(configOption);

        return options;
    }

    /**
     * 설정파일 로딩.
     * @param conf
     * @param optionValue
     */
    protected void loadConfig(Configuration conf, String optionValue) {
        File configPath = new File(optionValue);
        if (configPath.isDirectory()) {
            File[] configFiles = configPath.listFiles(new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    if (name != null) {
                        return name.endsWith(".xml");
                    }
                    return false;
                }
            });

            /** Path: Hadoop fs. */
            for (File file : configFiles) {
                conf.addResource(new Path(file.toString()));
            }

        } else {
            conf.addResource(optionValue);
        }
    }

    /**
     *
     * @param cmd
     * @param conf
     * @return
     */
    private boolean initJobInfo(CommandLine cmd, Configuration _conf) {

        LOG.info("[conf] :" + cmd.getOptionValue('c'));

        /** 아래 코드는 테스트 후 지울것. */
        String spark_master;
        spark_master = _conf.get(AnomaliesCouponConstant.SPARK_MASTER);
        if (spark_master != "") {
            _conf.set(AnomaliesCouponConstant.SPARK_MASTER, spark_master);
        } else {
            LOG.error("설정파일에 '" + AnomaliesCouponConstant.SPARK_MASTER  +  "'가 설정되어 있지 않습니다.(../conf/spo-config.xml 확인필요.)");
            return false;
        }

        /** --master yarn --deploy-mode client 시 사용하는 부분.*/
        /** 1. --num-executors = spark.executor.instances : executor 개수.*/
        String spark_executor_instances;
        spark_executor_instances = _conf.get(AnomaliesCouponConstant.SPARK_EXECUTOR_INSTANCES);
        if (spark_executor_instances != "") {
            _conf.set(AnomaliesCouponConstant.SPARK_EXECUTOR_INSTANCES, spark_executor_instances);
        } else {
            LOG.error("설정파일에 '" + AnomaliesCouponConstant.SPARK_EXECUTOR_INSTANCES  +  "'가 설정되어 있지 않습니다.(../conf/spo-config.xml 확인필요.)");
            return false;
        }
        /** 2. --executor-cores = spark.executor.cores : executor의 core 개수.*/
        String spark_executor_cores;
        spark_executor_cores = _conf.get(AnomaliesCouponConstant.SPARK_EXECUTOR_CORES);
        if (spark_executor_cores != "") {
            _conf.set(AnomaliesCouponConstant.SPARK_EXECUTOR_CORES, spark_executor_cores);
        } else {
            LOG.error("설정파일에 '" + AnomaliesCouponConstant.SPARK_EXECUTOR_CORES  +  "'가 설정되어 있지 않습니다.(../conf/spo-config.xml 확인필요.)");
            return false;
        }
        /** 3. --executor-memory = spark.executor.memory : executor의 memory 용량.*/
        String spark_executor_memory;
        spark_executor_memory = _conf.get(AnomaliesCouponConstant.SPARK_EXECUTOR_MEMORY);
        if (spark_executor_memory != "") {
            _conf.set(AnomaliesCouponConstant.SPARK_EXECUTOR_MEMORY, spark_executor_memory);
        } else {
            LOG.error("설정파일에 '" + AnomaliesCouponConstant.SPARK_EXECUTOR_MEMORY  +  "'가 설정되어 있지 않습니다.(../conf/spo-config.xml 확인필요.)");
            return false;
        }

        String spark_local_dir;
        spark_local_dir = _conf.get(AnomaliesCouponConstant.SPARK_LOCAL_DIR);
        if (spark_local_dir != "") {
            _conf.set(AnomaliesCouponConstant.SPARK_LOCAL_DIR, spark_local_dir);
        } else {
            LOG.error("설정파일에 '" + AnomaliesCouponConstant.SPARK_LOCAL_DIR  +  "'가 설정되어 있지 않습니다.(../conf/spo-config.xml 확인필요.)");
            return false;
        }

        return true;
    }

    /**
     * 설정파일에 대한 로딩 후 정상처리여부.
     * @param args
     * @return
     * @throws IOException
     */
    public boolean config(final String[] args) throws IOException {
        h_conf = new Configuration();

        Options options = makeOptions();
        GenericOptionsParser parser = new GenericOptionsParser(h_conf, options, args);

        CommandLine cmd = parser.getCommandLine();

        if (cmd == null) {
            return false;
        }

        // 설정파일들 로딩..
        if (cmd.hasOption('c')) {
            loadConfig(h_conf, cmd.getOptionValue('c'));
        }

        if (!initJobInfo(cmd, h_conf)) {
            return false;
        }

        return true;
    }
}
