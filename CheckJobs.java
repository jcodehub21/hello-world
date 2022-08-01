package org.jc.services;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.time.*;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.concurrent.TimeUnit;

/**
 * 2/15/20222 using ProcessHandle and ProcessHandle.Info interface (JDK 9 and above)
 * to gather process information on machines and kill parquet job processes older than two days
 * Also, check and send list of temp files that are currently on HDFS using ScheduledExecutorService timer
 * rhlhddfrd206: /apps/parquet_logs/jdk-9.0.4/bin/java -jar Jdk9Services-1.0.0-2-SNAPSHOT-all.jar /work/tsv
 * @author JaneCheng
 */
public class CheckJobs implements PathFilter{
    static String beginTable = "<table border=\"1\" style=\"font-family:Arial;font-size:9pt\"}>";
    static String endTable = "</table>";
    static StringBuilder sb = new StringBuilder();
    static LocalDate today;
    static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    static Configuration conf = new Configuration();
    static FileSystem fs = null;
    static Path path = null;
    static String hdfsPath;
    static boolean isEndTable = false;
    private static Logger logger = LogManager.getLogger(CheckJobs.class);

    public static void main(String[] args){
        today = LocalDate.now();
        hdfsPath = args[0];
        sb.append("Today's Date: " + today + "</br>");
        sb.append("Hostname: " + System.getenv("HOSTNAME") + "</br></br>");
        sb.append(beginTable);
        sb.append("<tr><td><b>User</b></td><td><b>Pid</b></td><td><b>Start Date</b></td><td><b>CommandLine</b></td></tr>");
        setConfResources();
        startTimer();

    }

    public static void setConfResources(){
        //need to add these resources because running a java class without spark
        //does not know that /work/tsv is part of hdfs
        conf.addResource(new Path("/etc/hadoop/conf.cloudera.yarn/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf.cloudera.yarn/hdfs-site.xml"));
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);
        //had to add the hadoop jars in build.gradle and set properties below due to error:  java.io.IOException: No FileSystem for scheme: hdfs
        conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
    }

    public static void startTimer(){

       /** scheduler.scheduleAtFixedRate(() ->{
            checkProcess();
        }, 12, 12, TimeUnit.HOURS);**/
       checkProcess();

    }

    public static void checkProcess(){
        //Lambda can be replaced with method reference
        //instead of using .forEach(process -> {}), you can use .forEach(Class::methodName) which will iterate each process to the methodName
        ProcessHandle.allProcesses().forEach(CheckJobs::processInfo);
        checkParquetTempFiles();
        // sb.append(endTable);
        sendEmail(sb);
    }
    public static void sendEmail(StringBuilder sb){

        try{
            String to = "janecheng@fico.com";
            String from = "janecheng@fico.com";
            String host = "mail.fairisaac.com";

            //create properties to store host and get or set the default mail session object
            Properties props = new Properties();
            props.put("mail.smtp.host", host);
            Session session = Session.getInstance(props);


            //create the message object
            MimeMessage msg = new MimeMessage(session);

            //set the sender's email address
            msg.setFrom(new InternetAddress(from));

            //set the recipient's email address
            msg.setRecipients(Message.RecipientType.TO, to);
            //set the subject heading
            msg.setSubject("BP Parquet Jobs Process");

            //set the date of sending the email; new date() initializes to current date
            msg.setSentDate(new Date());

            //set the message body; setText method only uses text/plain
            // msg.setText(msgBody);
            Multipart mp = new MimeMultipart();

            //set the html body part
            MimeBodyPart body = new MimeBodyPart();
            body.setContent(sb.toString(), "text/html");
            mp.addBodyPart(body);
            msg.setContent(mp);
            Transport.send(msg);

        }catch(Exception e){e.printStackTrace();}
    }

    public static void processInfo(ProcessHandle processhandle){

         try{
            if(processhandle.info().user().isPresent() && processhandle.info().user().get().trim().equalsIgnoreCase("fmtdata")) {
                if(processhandle.info().commandLine().isPresent() && processhandle.info().commandLine().get().contains("SPARK2")) {
                    sb.append("<tr><td>").append(processhandle.info().user().get()).append("</td>");
                    sb.append("<td>").append(processhandle.pid()).append("</td>");
                    sb.append("<td>").append((processhandle.info().startInstant().isPresent() ? processhandle.info().startInstant().get() : "-")).append("</td>");
                    sb.append("<td>").append((processhandle.info().commandLine().isPresent() ? processhandle.info().commandLine().get() : "-")).append("</td></tr>");

                    //another way to get the value of processhandle.info().arguments(); arguments() return Optional<String[]> arrays
                    /**process.info().arguments().ifPresent(arg -> {System.out.printf("%s%n", arg);});**/
                    analyzeProcessHandle(processhandle);
                }
            }
         }catch(Exception e){e.printStackTrace();}
    }

    public static void analyzeProcessHandle(ProcessHandle processhandle){
         try{
             sb.append(endTable);
             isEndTable = true;
             sb.append("</br>");
              LocalDate processDate = LocalDate.ofInstant(processhandle.info().startInstant().get(), ZoneOffset.UTC);
              if(processDate.isBefore(today)){
                  int yearDiff = today.getYear() - processDate.getYear();
                  int monthDiff = today.getMonthValue() - processDate.getMonthValue();
                  int dayDiff = today.getDayOfMonth() - processDate.getDayOfMonth();
                  String diff = "Process is " + yearDiff + " years, " + monthDiff + " months, and " + dayDiff + " days old.";
                  sb.append(diff).append("</br>");
                  if(dayDiff >= 2){
                      Process p = Runtime.getRuntime().exec("kill -9 " + processhandle.pid());  //Runtime works
                          //ProcessHandle.of(processhandle.pid()).ifPresent(ProcessHandle::destroy); //processhandle.destroy() does not work
                      sb.append("Process # killed: " + processhandle.pid());
                  }
              }
              sb.append("</br>");
         }catch(Exception e){e.printStackTrace();}
    }

    public static void checkParquetTempFiles(){
        PathFilter filter;
        FileStatus[] filesList;
        try{
            if(!isEndTable){  //table has not ended yet
                sb.append(endTable);
                sb.append("</br>");
            }
            sb.append(beginTable);
            sb.append("<tr><td><b>Access Time</b></td><td><b>File Name</b></td></tr>");
            //list all temp tsv files
            path = new Path(hdfsPath);
            fs = path.getFileSystem(conf);
            filter = new CheckJobs();
            filesList = fs.listStatus(path, filter);
            for(FileStatus file : filesList){
              sb.append("<tr><td>").append(LocalDateTime.ofInstant(Instant.ofEpochMilli(file.getAccessTime()),ZoneOffset.UTC)).append("</td><td>").append(file.getPath().getName()).append("</td></tr>");

            }
            sb.append(endTable);
        }catch(Exception e){e.printStackTrace();}
    }

    @Override
    public boolean accept(Path name) {
        return name.getName().endsWith(".temp.tsv.gz");
    }
}
