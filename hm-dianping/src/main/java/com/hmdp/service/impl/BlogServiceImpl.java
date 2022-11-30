package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    @Resource
    private IUserService userService;
    @Resource
    private IFollowService followService;

    @Autowired
    private StringRedisTemplate redisTemplate;

    // 根据id查询博客
    @Override
    public Result queryBlogById(Integer id) {
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("博客不存在！");
        }
        queryBlogUser(blog);
        // 查看当前用户是否进行了点赞
        isBlogLiked(blog);

        return Result.ok(blog);
    }


    // 首页热点博客
    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = this.query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            this.isBlogLiked(blog);
            this.queryBlogUser(blog);
        });
        return Result.ok(records);
    }

    // 对博客进行点赞
    @Override
    public Result likeBlog(Long id) {
        // 获取用户
        Long userId = UserHolder.getUser().getId();
        String key = RedisConstants.BLOG_LIKED + id;
        // 查看用户是否已经点赞
        Double score = redisTemplate.opsForZSet().score(key, userId.toString());
        // 当前用户没有点过赞
        if (score == null) {
            boolean isSuccess = this.update().setSql("liked = liked + 1").eq("id", id).update();
            // 如果点赞成功，把当前用户保存到 redis 中
            if (isSuccess) {
                redisTemplate.opsForZSet().add(key, userId.toString(), System.currentTimeMillis());
            }
        }
        // 当前用户已经点过赞
        else {
            boolean isSuccess = this.update().setSql("liked = liked - 1").eq("id", id).update();
            // 取消点赞成功，把当前用户从redis中删除
            if (isSuccess) {
                redisTemplate.opsForZSet().remove(key, userId.toString());
            }
        }
        return Result.ok();
    }

    /**
     * blog 的点赞排行榜
     *
     * @param id 查询的 blog id
     * @return 返回查询到的数据
     */
    @Override
    public Result queryBlogLikes(Long id) {
        // 查询的key
        String key = RedisConstants.BLOG_LIKED + id;
        // 获取前五个元素
        Set<String> range = redisTemplate.opsForZSet().range(key, 0, 5);
        // 特殊情况
        if (range == null || range.isEmpty()) {
            return Result.ok(Collectors.toList());
        }
        // 从set转为list
        List<Long> ids = range.stream().map(Long::valueOf).collect(Collectors.toList());
        // 拼接一下为了下面一句的设置条件
        String joinId = StrUtil.join(",", ids);
        // 从数据库中查询数据，并且转为想要的数据
        List<UserDTO> userDTOS = userService.query()
                .in("id", ids).last("ORDER BY FIELD(id," + joinId + ")").list()
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        // 返回结果
        return Result.ok(userDTOS);
    }

    @Override
    public Result saveBlog(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 保存探店博文
        boolean isSuccess = save(blog);
        // 如果没有保存成功
        if (!isSuccess) {
            return Result.fail("新增笔记失败！");
        }
        // 获取当前用户的所有粉丝
        List<Follow> follows = followService.query().eq("follow_user_id", user.getId()).list();
        follows.forEach(follow -> {
            // 获取粉丝id
            Long userId = follow.getUserId();
            // 推送，对粉丝的收件箱添加对应的博客的id
            redisTemplate.opsForZSet().add(RedisConstants.FEED + userId, blog.getId().toString(), System.currentTimeMillis());
        });
        return Result.ok(blog.getId());
    }


    /**
     * @param max 查询的时间的最大值
     * @param offset 偏移量
     * @return 查询到的博客数据以及偏移量以及本次查询到的数据的最小时间戳
     */
    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        // 1.先获取当前用户
        Long userId = UserHolder.getUser().getId();
        // 2.查询收件箱，从收件箱中获取关注的用户发的博客
        // 此处的三是每页显示的条数
        Set<ZSetOperations.TypedTuple<String>> typedTuples =
                redisTemplate.opsForZSet()
                        .reverseRangeByScoreWithScores(RedisConstants.FEED + userId, 0, max, offset, 3);
        // 如果收件箱中数据为空直接返回
        if (typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok();
        }
        // 存储所有的博客id
        List<Long> ids = new ArrayList<>(typedTuples.size());
        long minTime = 0;
        int os = 1;
        for (ZSetOperations.TypedTuple<String> type : typedTuples) {
            // 将string类型的id转为long类型
            ids.add(Long.valueOf(Objects.requireNonNull(type.getValue())));
            // 将对应的str时间戳转为long类型
            long time = Objects.requireNonNull(type.getScore()).longValue();
            if (time == minTime) {
                os++;
            } else {
                minTime = time;
                os = 1;
            }
        }
        String idStr = StrUtil.join(",", ids);
        // 查询所有博客
        List<Blog> blogs = query().in("id", ids)
                .last("ORDER BY FIELD(id," + idStr + ")").list();
        // 对博客进行完整的设置
        for (Blog blog : blogs) {
            // 查询blog有关的用户
            queryBlogUser(blog);
            // 查看当前用户是否进行了点赞
            isBlogLiked(blog);
        }
        // 数据封装
        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setList(blogs);
        scrollResult.setMinTime(minTime);
        scrollResult.setOffset(os);
        return Result.ok(scrollResult);
    }

    // 对博客的两个属性赋值
    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();        // 修改点赞数量
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

    private void isBlogLiked(Blog blog) {
        // 获取用户
        Long userId ;
        // 这里使用 try 的目的是当用户没有登陆时也能够浏览 blog
        // 但是不能够进行点赞，被拦截了
        try {
            userId = UserHolder.getUser().getId();
        } catch (NullPointerException e) {
            userId = -1L;
        }
        String key = RedisConstants.BLOG_LIKED + blog.getId();
        // 查看用户是否已经点赞
        Double score = redisTemplate.opsForZSet().score(key, userId.toString());
        // 设置点赞状态
        blog.setIsLike(score != null);
    }
}
