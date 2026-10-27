package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    private static final DefaultRedisScript<Long> BLOG_LIKE_SCRIPT;

    static {
        BLOG_LIKE_SCRIPT = new DefaultRedisScript<>();
        BLOG_LIKE_SCRIPT.setLocation(new ClassPathResource("blog-like.lua"));
        BLOG_LIKE_SCRIPT.setResultType(Long.class);
    }

    private final StringRedisTemplate stringRedisTemplate;
    private final IUserService userService;

    public BlogServiceImpl(StringRedisTemplate stringRedisTemplate, IUserService userService) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.userService = userService;
    }
    @Resource
    private IFollowService followService;

    @Override
    public Result likeBlog(Long id) {
        // 1、获取登录用户
        UserDTO user = UserHolder.getUser();
        if (user == null || user.getId() == null) {
            return Result.fail("请先登录");
        }
        Long userId = user.getId();

        // 2、用 Lua 原子地判断点赞状态并完成 ZSet 的添加或删除
        String key = BLOG_LIKED_KEY + id;
        Long action = stringRedisTemplate.execute(
                BLOG_LIKE_SCRIPT,
                Collections.singletonList(key),
                userId.toString(),
                String.valueOf(System.currentTimeMillis())
        );
        if (action == null) {
            return Result.fail("点赞操作失败");
        }

        // 3、Lua 返回 1 表示点赞，返回 -1 表示取消点赞
        boolean updated;
        if (action == 1L) {
            updated = update().setSql("liked = liked + 1").eq("id", id).update();
        } else {
            updated = update().setSql("liked = liked - 1").eq("id", id).gt("liked", 0).update();
        }
        if (!updated) {
            return Result.fail("点赞数量更新失败");
        }
        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Long id) {
        String key = BLOG_LIKED_KEY + id;
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        if (top5 == null || top5.isEmpty()){
            return Result.ok();
        }
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        String idStr = StrUtil.join(",",ids);
        List<UserDTO> userDTOS = userService.query()
                .in("id", ids)
                .last("ORDER BY FIELD(id, " + idStr + ")").list()
                .stream().map(user ->
                    BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(userDTOS);
    }

    @Override
    public Result saveBlog(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 保存探店博文
        boolean isSaved = save(blog);
        if (!isSaved) {
            return Result.fail("博客保存失败");
        }
        //获取关注的粉丝
        List<Follow> followers = followService.query().eq("follow_user_id", user.getId()).list();
        for (Follow follow : followers){
            //获取粉丝id
            Long userId = follow.getUserId();
            String key = FEED_KEY + userId;
            stringRedisTemplate.opsForZSet().add(key, blog.getId().toString(), System.currentTimeMillis());
        }
        // 返回id
        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        //1、获取当前用户
        Long userId = UserHolder.getUser().getId();
        String key = FEED_KEY + userId;
        //2、查询收件箱 ZREVRANGEBYSCORE KEY MAX MIN LIMIT offset,count
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(key, 0, max, offset, 3);
        if (typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok();
        }
        //3、解析数据 blogId，minTime，offset
        List<Long> blogIds = new ArrayList<>(typedTuples.size());
        long minTime = 0;
        int os = 1;
        for (ZSetOperations.TypedTuple<String> tuple : typedTuples) {
            blogIds.add(Long.valueOf(tuple.getValue()));
            long time = tuple.getScore().longValue();
            if (time == minTime) {
                os++;
            }
            else {
                minTime = time;
                os = 1;
            }
        }
        //4、根据id查询blog
        String idStr = StrUtil.join(",", blogIds);
        List<Blog> blogs = query().in("id", blogIds)
                .last("ORDER BY FIELD(id, " + idStr + ")").list();
        for (Blog blog : blogs) {
            getBlogById(blog);
            isBlogLiked(blog);
        }
        //5、封装返回
        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setList(blogs);
        scrollResult.setMinTime(minTime);
        scrollResult.setOffset(os);
        return Result.ok(scrollResult);
    }

    public Boolean isBlogLiked(Blog blog) {
        // /blog/hot 是免登录接口，未登录时按未点赞处理即可
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            blog.setIsLike(false);
            return false;
        }
        //2、判断当前用户是否已经点赞
        String key = BLOG_LIKED_KEY + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, user.getId().toString());
        blog.setIsLike(score != null);
        return blog.getIsLike();
    }

    public void getBlogById(Blog blog) {
        Long userId = blog.getUserId();
        // 查询用户
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
}
