package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOPTYPE_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_SHOPTYPE_TTL;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Override
    public Result queryTypeLisi() {
//        List<ShopType> typeList = typeService
//                .query().orderByAsc("sort").list();
        // 获取数据
        String jsonCache = redisTemplate.opsForValue().get(CACHE_SHOPTYPE_KEY);
        List<ShopType> shopTypes = JSONUtil.toList(jsonCache, ShopType.class);
        // 如果缓存不为空直接返回数据
        if (!shopTypes.isEmpty()) {
            return Result.ok(shopTypes);
        }
        // 缓存不存在的情况进行查询数据库
        shopTypes = query().orderByAsc("sort").list();
        // 存入缓存,并设置缓存时间
        redisTemplate.opsForValue().set(CACHE_SHOPTYPE_KEY, JSONUtil.toJsonStr(shopTypes),CACHE_SHOPTYPE_TTL, TimeUnit.MINUTES);
        return Result.ok(shopTypes);
    }
}
