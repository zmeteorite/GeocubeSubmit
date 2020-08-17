package whu.edu.cn.service;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.springframework.stereotype.Service;
import whu.edu.cn.config.spark.SparkAppParas;

import javax.servlet.http.HttpSession;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

@Service
public class SparkSubmitService {
    public String submitApp(SparkAppParas sparkAppParas,
                            String func,
                            String rasterProductName,
                            String extent,
                            String startTime,
                            String endTime,
                            String outputDir,
                            HttpSession session)
            throws IOException, InterruptedException{
        HashMap env = new HashMap();
        env.put("HADOOP_CONF_DIR", "/home/geocube/hadoop/etc/hadoop");
        env.put("JAVA_HOME", "/home/geocube/jdk1.8.0_131/");
        CountDownLatch countDownLatch = new CountDownLatch(1);

        SparkAppHandle handle = new SparkLauncher(env)
                .setSparkHome(sparkAppParas.getSparkHome())
                .setAppResource(sparkAppParas.getJarPath().get("maps.geocube"))
                .setMainClass(sparkAppParas.getMainClass().get("maps." + func))
                .addAppArgs(rasterProductName, extent, startTime, endTime, outputDir)
                .setMaster(sparkAppParas.getMaster())
                .setConf("spark.driver.memory", sparkAppParas.getDriverMemory())
                .setConf("spark.executor.memory", sparkAppParas.getExecutorMemory())
                .setConf("spark.cores.max", sparkAppParas.getTotalExecutorCores())
                .setConf("spark.executor.cores", sparkAppParas.getExecutorCores())
                .setConf("spark.kryoserializer.buffer.max", sparkAppParas.getKryoserializerBufferMax())
                .setConf("spark.rpc.message.maxSize", sparkAppParas.getRpcMessageMaxSize())
                .setVerbose(true).startApplication(new SparkAppHandle.Listener() {
                    int progressbarPercent = 0;
                    @Override
                    public void stateChanged(SparkAppHandle sparkAppHandle) {
                        if (sparkAppHandle.getState().isFinal()) {
                            countDownLatch.countDown();
                        }

                        if(sparkAppHandle.getState().toString() != "FINISHED" && progressbarPercent < 90) progressbarPercent += 10;
                        else if(sparkAppHandle.getState().toString() == "FINISHED") progressbarPercent = 100;
                        session.setAttribute("state", sparkAppHandle.getState().toString()+ "," + progressbarPercent + "%");

                        System.out.println("state:" + sparkAppHandle.getState().toString() + "," + progressbarPercent + "%");

                        /*File stateMonitor = new File(outputDir + "state");
                        try{
                            FileWriter writer = new FileWriter(stateMonitor, true);
                            writer.write("state:" + sparkAppHandle.getState().toString() + "\n");
                            writer.close();
                        }catch (IOException e) {
                        }*/
                    }

                    @Override
                    public void infoChanged(SparkAppHandle sparkAppHandle) {
                        System.out.println("Info:" + sparkAppHandle.getState().toString());
                    }
                });
        System.out.println("The task is executing, please wait ....");
        countDownLatch.await();
        System.out.println("The task is finished!");
        return outputDir;
    }
}
