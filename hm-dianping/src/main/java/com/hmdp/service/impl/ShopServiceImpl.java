package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.apache.ibatis.executor.ResultExtractor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        // 缓存穿透的版本
        // Shop shop = queryWithPassThrough(id);
        // 缓存击穿的版本
        Shop shop = queryWithMutex(id);
        // 逻辑过期的版本
//        Shop shop = queryWithLogicExpire(id);
        // 调用封装好的函数
//        Shop shop = cacheClient
//                .queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        if (shop == null) {
            return Result.fail("店铺不存在！");
        }
        return Result.ok(shop);
    }

    //线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    // 逻辑过期的版本
    public Shop queryWithLogicExpire(Long id) {
        // 1.从 redis 中查询缓存
        String shopJson = redisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        // 2.判断是否存在
        // 空的情况只有第一次访问时会为空 此时需要添加数据
        if (StrUtil.isBlank(shopJson)) {
            // 3.不存在添加数据后直接返回
            return saveShop2Redis(id, LOCK_SHOP_TTL);
        }
        // 4.命中，需要先把 json 反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 5.判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            //5.1未过期，直接返回店铺信息
            return shop;
        }
        // 5.2 已过期，需要缓存重建
        // 6.缓存重建
        // 6.1获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        // 6.2判断是否获取锁成功
        if (isLock) {
            // 6.3 成功开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    // 重建缓存
                    this.saveShop2Redis(id, LOCK_SHOP_TTL);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    // 释放锁
                    unLock(lockKey);
                }
            });
        }


        return shop;
    }

    //配套 逻辑过期使用
    private Shop saveShop2Redis(Long id, long expireSeconds) {
        // 1.查询店铺数据
        Shop shop = getById(id);
        // 2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        // 3.写入 Redis
        redisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
        return shop;

    }

    // 缓存击穿的版本
    public Shop queryWithMutex(Long id) {
        // 1.从 redis 中查询缓存
        String shopJson = redisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        // 2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            // 3.存在直接返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        // 为了解决缓存穿透 下面存入的是 "" isNotBlank 判断其为空
        if (shopJson != null) {
            return null;
        }
        // 4.实现缓存重建
        // 4.1 获取互斥锁
        String lockKey = "lock:shop" + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            // 4.2判断获取锁是否成功
            if (!isLock) {
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            // 4.3失败，休眠重试
            // 4.4成功根据 id 查询数据库
            shop = getById(id);
            // 5.不存在，返回错误
            if (shop == null) {
                // 为了解决缓存穿透，当前 id 不存在数据，就先存入一个空值，防止大量的查询给服务器造成压力
                redisTemplate.opsForValue()
                        .set(CACHE_SHOP_KEY + id, "", CACHE_SHOP_TTL, TimeUnit.MINUTES);
                return null;
            }
            // 6.存在，写入redis
            redisTemplate.opsForValue()
                    .set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            unLock(lockKey);
        }
        // 7.释放锁
        // 7.返回数据
        return shop;
    }

    // 获取锁
    private boolean tryLock(String key) {
        Boolean flag = redisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        // 不直接返回 flag 是防止拆箱造成空指针的情况
        return BooleanUtil.isTrue(flag);
    }

    // 释放锁
    private void unLock(String key) {
        redisTemplate.delete(key);
    }


    // 缓存穿透的版本
    public Shop queryWithPassThrough(Long id) {
        // 1.从 redis 中查询缓存
        String shopJson = redisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        // 2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            // 3.存在直接返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        // 为了解决缓存穿透 下面存入的是 "" isNotBlank 判断其为空
        if (shopJson != null) {
            return null;
        }
        // 4.不存在，根据id 查询数据库
        Shop shop = getById(id);
        // 5.不存在，返回错误
        if (shop == null) {
            // 为了解决缓存穿透，当前 id 不存在数据，就先存入一个空值，防止大量的查询给服务器造成压力
            redisTemplate.opsForValue()
                    .set(CACHE_SHOP_KEY + id, "", CACHE_SHOP_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 6.存在，写入redis
        redisTemplate.opsForValue()
                .set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        // 7.返回数据
        return shop;
    }


    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        // 更新数据先进行更新数据库，在进行缓存重建
        if (id == null) {
            return Result.fail("店铺不存在！");
        }
        updateById(shop);
        // 删除缓存
        redisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1.是否需要根据坐标查询
//        if (x == null || y == null) {
            Page<Shop> page = query().eq("type_id", typeId).page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            return Result.ok(page.getRecords());
//        }
//        // 下面才是按照地理坐标查的
//        // 2.计算分页参数
//        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
//        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
//        // 3.查询redis，按照距离排序、分页。结果：shopId、distance
//        String key = SHOP_GEO_KEY + typeId;
//        GeoResults<RedisGeoCommands.GeoLocation<String>> results = redisTemplate.opsForGeo()
//                .search(
//                        key,
//                        // 设置当前坐标位置
//                        GeoReference.fromCoordinate(x, y),
//                        // 设置搜索距离为 5000 m
//                        new Distance(5000),
//                        // 设置查询多少条 从 0 - end，没有方法能够直接查询 from - end
//                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
//                );
//        // 4.解析出id
//        if (results == null) {
//            // 没有数据直接返回
//            return Result.ok(Collections.emptyList());
//        }
//        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
//        if (list.size() < from) {
//            return Result.ok(Collections.emptyList());
//        }
//
//        // 4.1存储 from - end 的部分
//        List<Long> ids = new ArrayList<>(list.size());
//        // map中 key 对应 shop 的 id，value代表距离
//        Map<String, Distance> distanceMap = new HashMap<>(list.size());
//        // 4.1截取 from - end 的部分
//        list.stream().skip(from).forEach(result -> {
//            // 4.2获取店铺 id
//            String shopIdStr = result.getContent().getName();
//            ids.add(Long.valueOf(shopIdStr));
//            // 4.3获取距离
//            Distance distance = result.getDistance();
//            // 把 shop 和 距离一一对应起来
//            distanceMap.put(shopIdStr, distance);
//        });
//        // 5.根据id查询shop
//        String idStr = StrUtil.join(",", ids);
//        // 因为使用 in 语法并不能够保证顺序的问题，我们要使用 last 中的语句设置顺序的问题
//        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
//        // 对每一个店铺进行设置距离属性
//        for (Shop shop : shops) {
//            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
//        }
//        //6.返回
//        return Result.ok(shops);
    }
}
