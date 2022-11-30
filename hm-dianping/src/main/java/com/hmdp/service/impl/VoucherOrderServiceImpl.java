package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author 54656 on 2022/11/17 17:58
 */
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private StringRedisTemplate redisTemplate;

    private RedisIdWorker redisIdWorker = new RedisIdWorker(redisTemplate);

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        // 这个是基于阻塞队列的 lua 脚本
//        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        // 这个是基于 stream 的lua脚本
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckillStream.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }


    // 开启一个单独的线程
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    // 此注解是在当前类被springboot启动以后 直接执行的方法
    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable {
        String queueName ="stream.orders" ;

        @Override
        public void run() {
            // 此处虽然是一个死循环，不过不用很担心对 cpu 造成较大的负担，当阻塞线程内没东西时，他会卡在这里
            while (true) {
                try {
                    // 1.获取消息队列中的订单信息，XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS streams.order >
                    List<MapRecord<String, Object, Object>> list = redisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    // 2.判断消息获取是否成功
                    if (list == null || list.isEmpty()) {
                        // 如果获取失败，说明没有消息，进行下一次循环
                        continue;
                    }
                    // 3.解析消息中的订单消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 4.如果获取成功，可以下单
                    handleVoucherOrder(voucherOrder);
                    // 5.ACK确认 SACK stream.order g1 id
                    redisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    // 出现了异常说明ack确认失败，我们要去pending-list中取出区重试
                    log.error("处理订单异常", e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    // 1.获取 pending-list 中的订单信息，XREADGROUP GROUP g1 c1 COUNT 1 STREAMS streams.order 0
                    List<MapRecord<String, Object, Object>> list = redisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    // 2.判断消息获取是否成功
                    if (list == null || list.isEmpty()) {
                        // 如果获取失败，说明 pending-list 没有消息，结束循环
                        break;
                    }
                    // 3.解析消息中的订单消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 4.如果获取成功，可以下单
                    handleVoucherOrder(voucherOrder);
                    // 5.ACK确认 SACK stream.order g1 id
                    redisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    // 出现了异常说明ack确认失败，我们要去pending-list中取出区重试
                    log.error("处理pending-list订单异常", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
    }


    // 下面是阻塞队列使用的
/*    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            // 此处虽然是一个死循环，不过不用很担心对 cpu 造成较大的负担，当阻塞线程内没东西时，他会卡在这里
            while (true) {
                try {
                    // 1.获取阻塞队列中的订单信息
                    VoucherOrder voucherOrder = orderTasks.take();
                    // 2.创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (InterruptedException e) {
                    log.error("处理订单异常",e);
                }
            }
        }
    }*/

    // 基于stream消息队列实现
    @Override
    public Result seckillVoucher(Long voucherId) {
        // 获取用户
        Long userId = UserHolder.getUser().getId();
        // 生成订单id
        long orderId = redisIdWorker.nextId("order");
        // 1.执行 lua 脚本
        // 执行此脚本以后应该去看
        Long result = redisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );

        // 2.判断结果是否为0,即判断是否有购买资格
        assert result != null;
        int r = result.intValue();
        // 有购买资格才会返回订单好
        if (r != 0) {
            // 不为零，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足！" : "不能重复下单！");
        }
        // 实例化代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        return Result.ok(orderId);
    }


    // 代理对象
    private IVoucherOrderService proxy;
    // 优化后的版本 阻塞队列
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        // 获取用户
//        Long userId = UserHolder.getUser().getId();
//        // 1.执行 lua 脚本
//        Long result = redisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), userId.toString()
//        );
//        // 2.判断结果是否为0,即判断是否有购买资格
//        assert result != null;
//        int r = result.intValue();
//        if (r!=0) {
//            // 不为零，代表没有购买资格
//            return Result.fail(r == 1 ? "库存不足！" : "不能重复下单！");
//        }
//        //  2.2为0，有购买资格，把下单信息保存阻塞队列
//        VoucherOrder voucherOrder = new VoucherOrder();
//        // 2.3订单id
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        // 2.4用户id
//        voucherOrder.setUserId(userId);
//        // 2.5代金券id
//        voucherOrder.setVoucherId(voucherId);
//        // 2.6存入阻塞队列，当阻塞队列中存在对象时， VoucherOrderHandler 中的run方法会一直执行
//        orderTasks.add(voucherOrder);
//        // 3.获取代理对象
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//        // 3.返回订单id
//        return Result.ok(orderId);
//    }


    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        // 1.获取用户，此处不能够从 userHolder中去取了，因为这不是同一个线程
        Long userId = voucherOrder.getUserId();
        // 理论上此处不加锁也没有问题，为了保底
        // 2.创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        // 3.获取锁
        boolean isLock = lock.tryLock();
        if (!isLock) {
            log.error("不允许重复下单！");
            return;
        }
        try {
            proxy.createVoucherOrderAsync(voucherOrder);
        } finally {
            lock.unlock();
        }
    }


//    @Override
//    public Result seckillVoucher(Long voucherId) {
//
//        // 1.查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        // 2.判断秒杀是否开始
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            // 尚未开始
//            return Result.fail("秒杀尚未开始！");
//        }
//        // 3.判断秒杀是否已经结束
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
//            // 尚未开始
//            return Result.fail("秒杀已经结束！");
//        }
//        // 4.判断库存是否充足
//        if (voucher.getStock() < 1) {
//            // 库存不足
//            return Result.fail("库存不足！");
//        }
//        // 前置套件全部满足去此方法里面判断是否有锁，来实现真正的下单
//        return createVoucherOrder(voucherId);
//
//        /*非集群的情况
//        Long userId = UserHolder.getUser().getId();
//       synchronized (userId.toString().intern()) {
//         获取代理对象处理事务
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        }*/
//
///*      集群的情况
//        SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, redisTemplate);
//        boolean isLock = lock.tryLock(10);
//        if (!isLock) {
//            // 获取到锁，返回错误
//            return Result.fail("不允许重复下单！");
//        }
//
//        try {
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } finally {
//            lock.unlock();
//        }*/
//
//    }


    // 这个是异步的用法
    @Override
    public void createVoucherOrderAsync(VoucherOrder voucherOrder) {
        // 下面的判断都是以防万一
        Long userId = voucherOrder.getUserId();
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count > 0) {
            log.error("用户已经购买过一次！");
            return;
        }
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock", 0)
                .update();
        if (!success) {
            log.error("库存不足！");
            return;
        }
        // 创建订单
        save(voucherOrder);
    }

    @Resource
    private RedissonClient redissonClient;

    // 这个时 redisson 的用法
    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        // 5.一人一单
        Long userId = UserHolder.getUser().getId();

        // 创建锁对象
        RLock redisLock = redissonClient.getLock("lock:order:" + userId);
        // 尝试获取锁
        boolean isLock = redisLock.tryLock();
        // 判断，获取锁失败说明同一用户正在进行下单，处理并发返回失败
        if (!isLock) {
            return Result.fail("不允许重复下单！");
        }

        try {
            // 5.1查询订单，查询数据库中的订单是否含有此用户
            // count 的值只能为 1 或 0
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            // 5.2判断是否存在
            if (count > 0) {
                // 用户已经购买过了
                return Result.fail("用户已经购买了一次！");
            }
            // 6.扣减库存，当库存大于 0 的时候进行下单
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock -1")
                    .eq("voucher_id", voucherId).gt("stock", 0).update();
            // 扣减不成功说明库存不足
            if (!success) {
                // 扣减失败
                return Result.fail("库存不足！");
            }
            // 7.创建订单
            VoucherOrder voucherOrder = new VoucherOrder();
            // 7.1订单id,生成了订单号
            long orderId = redisIdWorker.nextId("order");
            voucherOrder.setId(orderId);
            // 7.2用户id
            voucherOrder.setUserId(userId);
            // 7.3 代金券id
            voucherOrder.setVoucherId(voucherId);
            save(voucherOrder);
            return Result.ok(orderId);
        }
        // 最后一定会释放锁
        finally {
            redisLock.unlock();
        }
    }

   /*   这个是不适用 redisson 的调用方法
    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        // 5.一人一单
        Long userId = UserHolder.getUser().getId();

        // 5.1查询订单，查询数据库中的订单是否含有此用户
        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        // 5.2判断是否存在
        if (count > 0) {
            // 用户已经购买过了
            return Result.fail("用户已经购买了一次！");
        }
        // 6.扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock -1")
                .eq("voucher_id", voucherId).gt("stock", 0).update();
        if (!success) {
            // 扣减失败
            return Result.fail("库存不足！");
        }
        // 7.创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        // 7.1订单id,生成了订单号
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        // 7.2用户id
        voucherOrder.setUserId(userId);
        // 7.3 代金券id
        voucherOrder.setVoucherId(voucherId);
        save(voucherOrder);
        return Result.ok(orderId);
    }*/
}
