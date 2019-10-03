package com.atguigu;

import com.sun.org.apache.regexp.internal.RE;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @Author cuihaiyan
 * @Create_Time 2019-09-30 00:26
 */
public class SQLSource extends AbstractSource implements Configurable, PollableSource {

    //打印日志
    private static final Logger LOG = LoggerFactory.getLogger(SQLSource.class);
    //定义sqlHelper
    private SQLSourceHelper sqlSourceHelper;

    @Override
    public void configure(Context context) {
        try {
            sqlSourceHelper = new SQLSourceHelper(context);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public Status process() throws EventDeliveryException {
        try {
            //查询数据表
            List<List<Object>> result = sqlSourceHelper.executeQuery();

            //存放event的集合
            ArrayList<Event> events = new ArrayList<>();

            //存放event的头集合
            HashMap<String, String> header = new HashMap<>();

            //如果有返回数据，就封装为event
            if (!result.isEmpty()) {
                List<String> rows = sqlSourceHelper.getAllRows(result);
                Event event = null;
                for (String row : rows) {
                    event = new SimpleEvent();
                    event.setBody(row.getBytes());
                    event.setHeaders(header);
                    events.add(event);
                }

                //将event写入channel
                this.getChannelProcessor().processEventBatch(events);

                //更新数据表中的offset信息
                sqlSourceHelper.updateOffset2DB(result.size());
            }

            //等待时长
            Thread.sleep(sqlSourceHelper.getRunQueryDelay());

            return Status.READY;
        } catch (Exception e) {
            LOG.error("Error processing row", e);
            //e.printStackTrace();
            return Status.BACKOFF;
        }
    }

    @Override
    public synchronized void stop() {
        LOG.info("stoping sql source {}...", getName());
        try {
            //关闭资源
            sqlSourceHelper.close();
        } finally {
            super.stop();
        }
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }


}
