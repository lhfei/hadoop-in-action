package cn.lhfei.hadoop.ch03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * Created by lhfei on 3/4/18.
 */
public class HDFSApi {

    static final Logger log = LoggerFactory.getLogger(HDFSApi.class);

    public static void main(String[] args) {

        String uri = "hdfs://master1.cloud.cn:9000/user" + "/lhfei";
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", HDFSConstant.FS_DEFAULTFS);
        FileSystem fs = null;

        try {
            fs = FileSystem.get(URI.create(uri), conf);

            Path src = new Path(uri + "/test/01/011/0111/01111");
            // make directory
            fs.mkdirs(src);
            fs.mkdirs(new Path(uri + "/test/02/022/0222/02222"));

            // put file
            fs.copyFromLocalFile(false, true, new Path("file:/home/lhfei/app_sdk/IP_List.txt"), new Path(uri));

            fs.delete(new Path(uri + "/test/01/011/0111/01111/011111"), false);

            fs.rename(new Path(uri + "/test/02/022/"), new Path(uri + "/test/02/1"));

            fs.delete(new Path(uri + "/app_sdk"), true);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
