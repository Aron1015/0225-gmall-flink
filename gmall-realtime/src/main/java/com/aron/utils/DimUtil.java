package com.aron.utils;


import com.alibaba.fastjson.JSONObject;
import com.aron.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

/**
 * select * from t where id='19' and name='zhangsan';
 * <p>
 * Redis:
 * 1.存什么数据？         维度数据   JsonStr
 * 2.用什么类型？         String  Set  Hash
 * 3.RedisKey的设计？     String：tableName+id  Set:tableName  Hash:tableName
 * t:19:zhangsan
 * <p>
 * 集合方式排除,原因在于我们需要对每条独立的维度数据设置过期时间
 */

public class DimUtil {
    public static JSONObject getDimInfo(Connection connection, String tableName, String id) {

        //查询Redis中的数据
        String redisKey = "DIM:"+tableName+":"+id;
        Jedis jedis = RedisUtil.getJedis();
        String dimInfo = jedis.get(redisKey);

        if (dimInfo != null) {
            JSONObject dimInfoJson = JSONObject.parseObject(dimInfo);

            //重置过期时间
            jedis.expire(redisKey, 24 * 60 * 60);
            jedis.close();
            return dimInfoJson;
        }

        //拼接SQL
        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id = '" + id + "'";
//        System.out.println("sql:" + querySql);

        //查询
        List<JSONObject> queryList = PhoenixUtil.queryList(connection, querySql, JSONObject.class, false);
        JSONObject dimIndoJson = queryList.get(0);

        //将查询到的数据写入缓存
        jedis.set(redisKey, dimIndoJson.toJSONString());

        //设置过期时间
        jedis.expire(redisKey, 24 * 60 * 60);
        jedis.close();

        return dimIndoJson;
    }

    public static void delDimInfo(String redisKey) {
        //获取Redis连接
        Jedis jedis = RedisUtil.getJedis();
        //删除数据
        jedis.del(redisKey);
        //归还连接
        jedis.close();
    }

    public static void main(String[] args) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        long start = System.currentTimeMillis();
        System.out.println(getDimInfo(connection, "DIM_USER_INFO", "3"));
        long stop1 = System.currentTimeMillis();
        System.out.println(getDimInfo(connection, "DIM_USER_INFO", "3"));
        long stop2 = System.currentTimeMillis();

        System.out.println(stop1-start);
        System.out.println(stop2-stop1);

        connection.close();

    }
}
