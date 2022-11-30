package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.conditions.query.QueryChainWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.BlogComments;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.BlogCommentsMapper;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IBlogCommentsService;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author 54656 on 2022/11/20 17:56
 */
@Service
public class IFollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

   @Autowired
   private StringRedisTemplate redisTemplate;

   @Autowired
   private IUserService userService;

    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            return Result.fail("请登陆账号！");
        }
        String key = RedisConstants.FOLLOWS_ID + user.getId();
        // 判断关注还是取关
        if (isFollow) {
            Follow follow = new Follow();
            follow.setUserId(user.getId());
            follow.setFollowUserId(followUserId);
            boolean isSuccess = save(follow);
            // 如果添加关注成功，就把被关注者放入set集合中
            if (isSuccess) {
                // set 集合是当前用户的关注用户集合
                redisTemplate.opsForSet().add(key, followUserId.toString());
            }
        }
        // 取消关注
        else {
            boolean isSuccess = remove(new QueryWrapper<Follow>()
                    .eq("user_id", user.getId()).eq("follow_user_id", followUserId));
            // 如果取消关注，就把被关注者从关注列表中删除
            if (isSuccess) {
                redisTemplate.opsForSet().remove(key, followUserId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result isFollow(Long followUserId) {
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            return Result.fail("未关注！");
        }
        Integer count = query().eq("user_id", user.getId()).eq("follow_user_id", followUserId).count();
        return Result.ok(count > 0);
    }

    @Override
    public Result followCommons(Long id) {
        UserDTO userDTO = UserHolder.getUser();
        if (userDTO == null) {
            // 用户未登录 返回一个空集合
            return Result.ok(Collections.emptyList());
        }
        Long userId = userDTO.getId();
        String key1 = RedisConstants.FOLLOWS_ID + userId;
        String key2 = RedisConstants.FOLLOWS_ID + id;
        Set<String> intersect = redisTemplate.opsForSet().intersect(key1, key2);
        // 没有共同关注
        if (intersect == null || intersect.isEmpty()) {
            // 返回一个空集合
            return Result.ok(Collections.emptyList());
        }
        // 先将 string 类型 转为 long 类型
        List<Long> ids = intersect.stream().map(Long::valueOf).collect(Collectors.toList());
        // 从ids中查询所有的用户
        List<UserDTO> users = userService.listByIds(ids).stream().map(user -> BeanUtil.copyProperties(user, UserDTO.class)).collect(Collectors.toList());
        return Result.ok(users);
    }
}
