package api;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.lizhi.pplive.PPliveBusiness;
import com.netflix.governator.annotations.AutoBindSingleton;
import com.yibasan.lizhifm.protocol.LZModelsPtlbuf;
import com.yibasan.lizhifm.protocol.LZUserSyncPtlbuf;
import com.yibasan.lizhifm.protocol.LizhiFMPtlbuf;
import fm.lizhi.commons.service.client.pojo.Result;
import fm.lizhi.hy.social.vo.LiveBondVO;
import fm.lizhi.hy.social.vo.LiveMicBondVO;
import fm.lizhi.live.amusement.config.LiveAmusementConfig;
import fm.lizhi.live.amusement.dto.PushLiveFunDataDto;
import fm.lizhi.live.amusement.hy.dto.AmusementPushEvent;
import fm.lizhi.live.amusement.hy.dto.WaitingUserEvent;
import fm.lizhi.live.amusement.hy.protocol.LiveAmusementProto;
import fm.lizhi.live.amusement.hy.protocol.NewLiveAmusementProto;
import fm.lizhi.live.amusement.kafka.BusinessEventProducer;
import fm.lizhi.live.amusement.local.cache.CachePopularityServiceManager;
import fm.lizhi.live.amusement.util.LzProtocolCodec;
import fm.lizhi.live.push.api.PushService;
import fm.lizhi.live.push.api.StudioLiveService;
import fm.lizhi.live.push.protocol.PushServiceProto;
import fm.lizhi.live.room.hy.api.LiveRoomService;
import fm.lizhi.live.room.hy.api.PopularityService;
import fm.lizhi.live.room.hy.protocol.LivePopularityProto.ResponseGetPopularityGroupCount;
import fm.lizhi.live.room.hy.protocol.LiveRoomProto;
import fm.lizhi.pp.util.utils.EnvUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.skywalking.apm.toolkit.trace.RunnableWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static fm.lizhi.live.amusement.constant.HostSeatConstants.SEAT_EMPTY;

@AutoBindSingleton
public class PushManager {
    private static Logger pushLogger = LoggerFactory.getLogger("push_logger");

    private static Logger logger = LoggerFactory.getLogger(PushManager.class);

    private static final AtomicLong atomic = new AtomicLong(0);

    //延迟推送任务队列
    private DelayQueue<PushTask> pushTaskDelayQueue = new DelayQueue<PushTask>();

    //延迟推送任务队列检测线程
    private Thread daemonThread;

    @Inject
    private CachePopularityServiceManager cachePopularityServiceManager;

    @Inject
    private AmusementRedisManager amusementRedisManager;

    @Inject
    private LiveAmusementConfig liveAmusementConfig;

    @Inject
    private PushService messagePushService;

    @Inject
    private PushAdapterManager pushAdapterManager;

    @Inject
    private StudioLiveService studioLiveService;

    @Inject
    private PushCacheManager pushCacheManager;

    @Inject
    private ExecutorManager executorManager;

    @Inject
    private LiveRoomService liveRoomService;

    @Inject
    private HostSeatRedisManager hostSeatRedisManager;

    @Inject
    private LiveAmusemenPlayGameManager liveAmusemenPlayGameManager;
    @Inject
    private BondManager bondManager;

    @Inject
    private BusinessEventProducer businessEventProducer;

    @PostConstruct
    public void init() {
        daemonThread = new Thread(new Runnable() {

            @Override
            public void run() {
                executePushTask();
            }
        });
        daemonThread.setName("PushQueue-Daemon-Thread");
        daemonThread.setDaemon(true);
        daemonThread.start();
    }

    private void executePushTask() {
        pushLogger.info("start executePushTask method!");
        while (true) {
            try {
                PushTask<Runnable> pushTask = pushTaskDelayQueue.take();
                Runnable realTask = pushTask.t;
                if (realTask != null) {
                    executorManager.execute(RunnableWrapper.of(realTask));
                    pushLogger.info("execute delayed pushTask expireTime:{}", pushTask.expireTime);
                }
            } catch (InterruptedException e) {
                logger.error("Daemon Thread has interrupted!", e);
            }
        }
    }

    ExecutorService executors = Executors.newFixedThreadPool(10, new ThreadFactory() {

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName("push-manager-thread");
            return thread;
        }
    });

    /**
     * 创建直播娱乐模式数据(liveFunData)异步推送任务
     *
     * @param liveId 直播节目ID
     */
    public void createLiveFunDataPushTask(long liveId) {
        LiveFunDataPushTask liveFunDataPushTask = new LiveFunDataPushTask(liveId);
        executorManager.execute(liveFunDataPushTask);
    }

    /**
     * 创建直播娱乐模式数据(liveFunData)异步推送任务
     *
     * @param liveId 直播节目ID
     */
    public void createLiveFunDataDealayedPushTask(long liveId) {
        //try {
        //    long nowTime = System.currentTimeMillis();
        //    if (pushCacheManager.checkLiveFunLocalLastTriggerTime(liveId, nowTime)) {
        //        logger.info("checkLiveFunLocalLastTriggerTime not pass for liveId:{}, nowTime:{}", liveId, nowTime);
        //        if (pushCacheManager.isTheSameTriggerPushPeriod(liveId, nowTime)) {
        //            return;
        //        }
        //        logger.info("not the same trigger push period for liveId:{}, nowTime:{}", liveId, nowTime);
        //    }
        //    long lastTriggerTime = amusementRedisManager.getPushLastTriggerTime(liveId);
        //    if (isTriggerFrequency(nowTime, lastTriggerTime)) {
        //        pushCacheManager.synchronLiveFunSharedLastTriggerTime(liveId, lastTriggerTime + liveAmusementConfig.getPushInterval());
        //        logger.info("liveFun push trigger too frequency for liveId:{}, lastTriggerTime:{}", liveId, lastTriggerTime);
        //        return;
        //    }
        //    boolean result = amusementRedisManager.updatePushLastTriggerTime(liveId, nowTime);
        //    if (!result) {
        //        return;
        //    }
        //    long expireTime = nowTime + liveAmusementConfig.getPushInterval();
        //    pushCacheManager.synchronLiveFunSharedLastTriggerTime(liveId, expireTime);
        //    LiveFunDataPushTask liveFunDataPushTask = new LiveFunDataPushTask(liveId);
        //    pushTaskDelayQueue.put(new PushTask<Runnable>(liveFunDataPushTask, expireTime));
        //    pushLogger.info("create liveFun push task for liveId:{}, expireTime:{}", liveId, expireTime);
        //} catch (Exception e) {
        //    logger.error("create liveFun push task occur ex for liveId:{}", liveId);
        //}
        createLiveFunDataDealayedPushTask(liveId, liveAmusementConfig.getPushInterval());
    }

    /**
     * 创建直播娱乐模式数据(liveFunData)异步推送任务
     *
     * @param liveId 直播节目ID
     */
    public void createLiveFunDataDealayedPushTask(long liveId, long pushInterval) {
        try {
            long nowTime = System.currentTimeMillis();
            if (pushCacheManager.checkLiveFunLocalLastTriggerTime(liveId, nowTime)) {
                logger.info("checkLiveFunLocalLastTriggerTime not pass for liveId:{}, nowTime:{}", liveId, nowTime);
                if (pushCacheManager.isTheSameTriggerPushPeriod(liveId, nowTime)) {
                    return;
                }
                logger.info("not the same trigger push period for liveId:{}, nowTime:{}", liveId, nowTime);
            }
            long lastTriggerTime = amusementRedisManager.getPushLastTriggerTime(liveId);
            if (isTriggerFrequency(nowTime, lastTriggerTime)) {
                pushCacheManager.synchronLiveFunSharedLastTriggerTime(liveId, lastTriggerTime + pushInterval);
                logger.info("liveFun push trigger too frequency for liveId:{}, lastTriggerTime:{}", liveId, lastTriggerTime);
                return;
            }
            boolean result = amusementRedisManager.updatePushLastTriggerTime(liveId, nowTime);
            if (!result) {
                return;
            }
            long expireTime = nowTime + pushInterval;
            pushCacheManager.synchronLiveFunSharedLastTriggerTime(liveId, expireTime);
            LiveFunDataPushTask liveFunDataPushTask = new LiveFunDataPushTask(liveId);
            pushTaskDelayQueue.put(new PushTask<Runnable>(RunnableWrapper.of(liveFunDataPushTask), expireTime));

            sendAmusementPushMsg(liveId);
            pushLogger.info("create liveFun push task for liveId:{}, expireTime:{}", liveId, expireTime);
        } catch (Exception e) {
            logger.error("create liveFun push task occur ex for liveId:{}", liveId);
        }
    }

    private void sendAmusementPushMsg(long liveId) {
        AmusementPushEvent event = new AmusementPushEvent();
        event.setLiveId(liveId);
        businessEventProducer.sendAmusementEvent(event);
    }

    /**
     * 创建直播娱乐模式数据(livePrompt)异步推送任务
     *
     * @param liveId:主播间ID
     * @param type:提示类型
     * @param msg:提示内容
     * @param action:Action跳转
     * @param userId:推送消息目标用户ID
     */
    public void createLivePromptPushTask(long liveId, int type, String msg, String action, long userId) {
        LivePromptPushTask livePromptPushTask = new LivePromptPushTask(liveId, type, msg, action, userId);
        executorManager.execute(livePromptPushTask);
    }

    /**
     * 向设置的推送消息
     *
     * @param njId
     * @param uid
     */
    public void pushRoomTip(long njId, long uid, String msg, long liveId) {
        try {
            if (StringUtils.isBlank(msg)) {
                logger.warn("msg is null msg:{},njId:{},uid:{}", msg, njId, uid);
                return;
            }

            // 推送提示
            this.createLivePromptDelayPushTask(liveId, 0, msg, null, uid);
        } catch (Exception e) {
            logger.error("push handle tip to acc error, njId:{}, userId:{}", njId, uid, e);
        }
    }

    /**
     * 创建直播娱乐模式数据(livePrompt)异步延迟推送任务
     *
     * @param liveId:主播间ID
     * @param type:提示类型
     * @param msg:提示内容
     * @param action:Action跳转
     * @param userId:推送消息目标用户ID
     */
    public void createLivePromptDelayPushTask(long liveId, int type, String msg, String action, long userId) {
        pushLogger.info("livePrompt delay push task for liveId:{}, start", liveId);
        try {
            long nowTime = System.currentTimeMillis();
            if (pushCacheManager.checkLivePromptLocalLastPushTime(userId, nowTime)) {
                logger.info("checkLivePromptLocalLastPushTime not pass for userId:{}", userId);
                return;
            }
//            long lastTriggerTime = amusementRedisManager.getPushLastTriggerTime(userId);
//            if (isTriggerFrequency(nowTime, lastTriggerTime)) {
//                logger.info("livePrompt delay push trigger too frequency for userId:{}", userId);
//                return;
//            }
//            amusementRedisManager.updatePushLastTriggerTime(userId, nowTime);
            long expireTime = nowTime + liveAmusementConfig.getPushInterval();
            LivePromptPushTask livePromptPushTask = new LivePromptPushTask(liveId, type, msg, action, userId);
            pushTaskDelayQueue.put(new PushTask<Runnable>(livePromptPushTask, expireTime));
            pushLogger.info("livePrompt delay push task for liveId:{}, expireTime:{}, finish", liveId, expireTime);
        } catch (Exception e) {
            logger.error("create livePrompt delay push task occur ex for liveId:{}", liveId);
        }
    }

    /**
     * 创建直播娱乐模式数据(myRole)异步推送任务
     *
     * @param userId:推送消息目标用户ID
     */
    public void createMyRolePushTask(long njId, long userId, int operation) {
        MyRolePushTask myRolePushTask = new MyRolePushTask(njId, userId, operation);
        executorManager.execute(myRolePushTask);
    }

    /**
     * 创建直播娱乐模式数据(myRole)异步延迟推送任务
     *
     * @param userId:推送消息目标用户ID
     */
    public void createMyRoleDelayPushTask(long njId, long userId, int operation) {
        pushLogger.info("myRole delay push task for userId:{}, operation:{}, start", userId, operation);
        if (!liveAmusementConfig.isPushAmusementSwitch()) {
            pushLogger.info("pushAmusementSwitch is close, cancel push myRole for userId:{}, operation:{}", userId, operation);
            return;
        }
        try {
            long nowTime = System.currentTimeMillis();
            long expireTime = nowTime + liveAmusementConfig.getPushInterval();
            MyRolePushTask myRolePushTask = new MyRolePushTask(njId, userId, operation);
            pushTaskDelayQueue.put(new PushTask<Runnable>(myRolePushTask, expireTime));
            pushLogger.info("myRole delay push task for njId:{}, userId:{}, operation:{}, expireTime:{} finish", njId, userId, operation, expireTime);
        } catch (Exception e) {
            logger.error("create myRole delay push task occur ex for njId:{}, userId:{}, operation:{}", njId, userId, operation);
        }
    }

    /**
     * 创建直播娱乐模式轮播厅数据(HostSeatRole)异步延迟推送任务
     *
     * @param userId:推送消息目标用户ID
     */
    public void createHostSeatRoleDelayPushTask(long njId, long userId, int operation) {
        pushLogger.info("hostSeatRole delay push task for njId={}, userId={}, operation={}, start", njId, userId, operation);
        if (!liveAmusementConfig.isPushAmusementSwitch()) {
            pushLogger.info("pushAmusementSwitch is close, cancel push hostSeatRole for userId={}, operation={}", userId, operation);
            return;
        }
        try {
            long nowTime = System.currentTimeMillis();
            long expireTime = nowTime + liveAmusementConfig.getPushInterval();
            HostSeatRolePushTask hostSeatRolePushTask = new HostSeatRolePushTask(njId, userId, operation);
            pushTaskDelayQueue.put(new PushTask<Runnable>(hostSeatRolePushTask, expireTime));
            pushLogger.info("hostSeatRole delay push task for njId={}, userId={}, operation={}, expireTime={} finish", njId, userId, operation, expireTime);
        } catch (Exception e) {
            logger.error("create hostSeatRole delay push task occur ex for njId=P{, userId={}, operation={}", njId, userId, operation);
        }
    }

    /**
     * 创建直播娱乐模式排麦用户列表(liveFunWaitingUsers)异步推送任务
     *
     * @param liveId 直播节目ID
     * @param njId   嘉宾用户ID
     */
    public void createWaitingUsersDelayedPushTask(long liveId, long njId) {
        if (!liveAmusementConfig.isPushAmusementSwitch()) {
            pushLogger.info("pushAmusementSwitch is close, cancel push waitUser for liveId:{}", liveId);
            return;
        }
        try {
            long nowTime = System.currentTimeMillis();
            if (pushCacheManager.checkWaitUserLocalLastPushTime(liveId, nowTime)) {
                logger.info("checkWaitUserLocalLastPushTime not pass for liveId:{}", liveId);
                return;
            }
            long lastPushTime = amusementRedisManager.getWaitingUsersPushTime(liveId);
            if (isTriggerFrequency(nowTime, lastPushTime)) {
                logger.info("waitUser trigger push too frequency for liveId:{}", liveId);
                return;
            }
            amusementRedisManager.updateWaitingUsersPushTime(liveId, nowTime);
            long expireTime = nowTime + liveAmusementConfig.getPushInterval();
            WaitingUsersPushTask waitingUsersPushTask = new WaitingUsersPushTask(liveId, njId, hostSeatRedisManager.getHostSeatUserId(liveId));
            pushTaskDelayQueue.put(new PushTask<Runnable>(waitingUsersPushTask, expireTime));
            sendWaitingUserMsg(liveId);
            pushLogger.info("create waitUser push task for liveId:{}, expireTime:{}", liveId, expireTime);
        } catch (Exception e) {
            logger.error("create waitUser push task occur ex for liveId:{}", liveId);
        }
    }

    private void sendWaitingUserMsg(long liveId) {
        WaitingUserEvent event = new WaitingUserEvent();
        event.setLiveId(liveId);
        businessEventProducer.sendWaitingUserEvent(event);
    }

    /**
     * 创建用户被踢出直播间(liveRoomKickOut)异步推送任务
     * @param liveId 直播节目ID
     * @param userId 用户ID
     */
    public void createLiveRoomKickOutPushTask(long liveId, long userId) {
        LiveRoomKickOutUserTask task = new LiveRoomKickOutUserTask(liveId, userId);
        executorManager.execute(task);
    }

    /**
     * 创建直播建撤回评论推送任务
     * @param liveId    直播id
     * @param commentId 评论id
     */
    public void createRevertLiveCommentTask(long liveId, long commentId) {
        LiveCommentRevertTask task = new LiveCommentRevertTask(liveId, commentId);
        executorManager.execute(task);
    }

    private boolean isSupportPush(long liveId) {
        // 直播人气服务
        try {
            PopularityService cachePopularityService =
                    cachePopularityServiceManager.getCachePopularityService();
            Result<ResponseGetPopularityGroupCount> result =
                    cachePopularityService.getPopularityGroupCount(liveId);
            if (result.rCode() == 0) {
                int liveOnlineUserNum = result.target().getCurrentCount();
                if (liveOnlineUserNum > liveAmusementConfig.getPushOnlineUserNumLimited()) {
                    return false;
                }
            } else {
                logger.error("cachePopularityService.getPopularityGroupCount error , liveId={} , rCode={}",
                        liveId, result.rCode());
            }
            return true;
        } catch (Exception e) {
            logger.error(String.format("getPopularityGroupCount error , liveId=%d", liveId), e);
        }

        logger.error("isSupportPush error , liveId={}", liveId);
        throw new RuntimeException("isSupportPush error");
    }

    private boolean isTriggerFrequency(long nowTime, long lastTriggerTime) {
        if ((nowTime - lastTriggerTime) >= liveAmusementConfig.getPushInterval()) {
            return false;
        }
        return true;
    }



    /**
     * <b><code>LiveFunDataPushTask</code></b>
     * <p>
     * 直播娱乐模式数据liveFunData推送任务封装类
     * </p>
     * <b>Creation Time:</b> 2017年10月30日 下午4:24:10
     *
     * @author Zbean
     */
    private class LiveFunDataPushTask implements Runnable {
        //直播节目ID
        private long liveId;

        public LiveFunDataPushTask(long liveId) {
            this.liveId = liveId;
        }

        @Override
        public void run() {
            try {
                long nowTime = System.currentTimeMillis();
                amusementRedisManager.updateLiveFunPushTime(liveId, nowTime);
                if (!liveAmusementConfig.isPushAmusementSwitch()) {
                    pushLogger.info("pushAmusementSwitch is close, cancel push liveFun for liveId:{}", liveId);
                    return;
                }
                if (!isSupportPush(liveId)) {
                    pushLogger.info("beyond push online user num limited for liveId:{}, cancel this time push!", liveId);
                    return;
                }
                PushLiveFunDataDto pushDto = pushAdapterManager.buildLiveFunData(liveId, nowTime);
                LZModelsPtlbuf.liveFunData liveFunData = pushDto.getLiveFunData();
                LZUserSyncPtlbuf.pushLiveGeneralData.Builder pushLiveGeneralData = LZUserSyncPtlbuf.pushLiveGeneralData.newBuilder();
                pushLiveGeneralData.setType(1);
                pushLiveGeneralData.setLiveGeneralData(pushAdapterManager.buildLiveGeneralData(0, liveFunData.toByteString()));
                Result<Void> result = studioLiveService.sendLivePushMsg(liveId, pushAdapterManager.generatePushLiveGeneralData(pushLiveGeneralData.build()));
                pushLiveBond(liveId, pushDto.getBlindDate(), pushDto.getLiveFunSeats());
                pushLogger.info("push liveFun task for liveId:{}, rCode:{}", liveId, result.rCode());
            } catch (Exception e) {
                logger.error("push liveFun occur ex for liveId{}, e:{}", liveId, e);
            }
        }
    }


    /**
     * livePromptPushTask
     * 直播娱乐模式数据livePrompt推送任务封装类
     */
    private class LivePromptPushTask implements Runnable {
        /**
         * 直播节目ID
         */
        private long liveId;
        /**
         * 提示类型
         */
        private int type;
        /**
         * 提示内容
         */
        private String msg;
        /**
         * Action 跳转
         */
        private String action;
        /**
         * 目标推送用户ID
         */
        private long userId;

        public LivePromptPushTask(long liveId, int type, String msg, String action, long userId) {
            this.liveId = liveId;
            this.type = type;
            this.msg = msg;
            this.action = action;
            this.userId = userId;
        }

        @Override
        public void run() {
            try {
                pushLogger.info("push livePrompt task for liveId:{}, userId:{}, start", liveId, userId);
                long nowTime = System.currentTimeMillis();
                if (!liveAmusementConfig.isPushAmusementSwitch()) {
                    pushLogger.info("pushAmusementSwitch is close, cancel push livePrompt for liveId:{}, userId:{}", liveId, userId);
                    return;
                }
                LZModelsPtlbuf.livePrompt livePrompt = pushAdapterManager.buildLivePromt(liveId, type, msg, action, nowTime);
                LZUserSyncPtlbuf.pushLiveGeneralData.Builder pushLiveGeneralData = LZUserSyncPtlbuf.pushLiveGeneralData.newBuilder();
                pushLiveGeneralData.setType(5);
                pushLiveGeneralData.setLiveGeneralData(pushAdapterManager.buildLiveGeneralData(0, livePrompt.toByteString()));
                int rCode;
                //if (liveAmusementConfig.isOpenDegrade()) {
                //    Result<Long> result = messagePushService.pushMessage2User(userId, pushAdapterManager.generatePushLiveGeneralData(pushLiveGeneralData.build()));
                //    rCode = result.rCode();
                //} else {
                //    Result<Void> result = messagePushApi.pushMessage2UserAsync(Arrays.asList(userId), pushAdapterManager.generatePushLiveGeneralData(pushLiveGeneralData.build()));
                //    rCode = result.rCode();
                //}
                Result<PushServiceProto.ResponsePushMessage2UserByte> result = messagePushService.pushMessage2UserByte(userId, pushAdapterManager.generatePushLiveGeneralData(pushLiveGeneralData.build()));
                rCode = result.rCode();
                pushLogger.info("push livePrompt task for liveId:{}, userId:{}, rCode:{}, finish", liveId, userId, rCode);
            } catch (Exception e) {
                logger.error("push livePrompt occur ex for liveId{}, userId:{}", liveId, userId, e);
            }
        }
    }

    /**
     * pushMyRole
     * 推送我的用户角色
     */
    private class MyRolePushTask implements Runnable {
        /**
         * 主播ID
         */
        private long njId;

        /**
         * 目标推送用户ID
         */
        private long userId;

        /**
         * 设置类型
         */
        private int operation;

        public MyRolePushTask(long njId, long userId, int operation) {
            this.njId = njId;
            this.userId = userId;
            this.operation = operation;
        }

        @Override
        public void run() {
            pushLogger.info("push myRole task for njId:{}, userId:{}, operation:{}, start", njId, userId, operation);
            try {
                long radioId = getRadioByUserId(njId);
                pushLogger.info("push myRole task for njId:{}, userId:{}, operation:{}, radioId:{}", njId, userId, operation, radioId);
                LZUserSyncPtlbuf.pushMyRole.Builder pushMyRoleBuilder = LZUserSyncPtlbuf.pushMyRole.newBuilder();

                LZModelsPtlbuf.roleInfo.Builder roleInfoBuilder = LZModelsPtlbuf.roleInfo.newBuilder();
                roleInfoBuilder.setRoleType(6);
                roleInfoBuilder.setOperation(operation);

                LZModelsPtlbuf.userRole.Builder userRoleBuilder = LZModelsPtlbuf.userRole.newBuilder();
                userRoleBuilder.addRoles(roleInfoBuilder);
                userRoleBuilder.setTargetId(radioId);
                userRoleBuilder.setTargetType(1);
                pushMyRoleBuilder.setUserRole(userRoleBuilder);

                int rCode;
                //if (liveAmusementConfig.isOpenDegrade()) {
                //    Result<Long> result = messagePushService.pushMessage2User(userId, LzProtocolCodec.generatePushMyRoleData(pushMyRoleBuilder.build()));
                //    rCode = result.rCode();
                //} else {
                //    Result<Void> result = messagePushApi.pushMessage2UserAsync(Arrays.asList(userId), LzProtocolCodec.generatePushMyRoleData(pushMyRoleBuilder.build()));
                //    rCode = result.rCode();
                //}
                Result<PushServiceProto.ResponsePushMessage2UserByte> result = messagePushService.pushMessage2UserByte(userId, LzProtocolCodec.generatePushMyRoleData(pushMyRoleBuilder.build()));
                rCode = result.rCode();
                pushLogger.info("push myRole task for njId:{}, userId:{}, operation:{}, rCode:{}, finish", njId, userId, operation, rCode);
            } catch (Exception e) {
                logger.error("push myRole occur ex for njId:{}, userId{}, operation:{}", njId, userId, operation, e);
            }
        }
    }

    /**
     * pushMyRole
     * 推送我的用户角色 -- 主持位
     */
    private class HostSeatRolePushTask implements Runnable {
        /**
         * 主播ID
         */
        private long njId;
        /**
         * 目标推送用户ID
         */
        private long userId;
        /**
         * 设置类型
         */
        private int operation;

        public HostSeatRolePushTask(long njId, long userId, int operation) {
            this.njId = njId;
            this.userId = userId;
            this.operation = operation;
        }

        @Override
        public void run() {
            pushLogger.info("push hostSeatRole task for njId={}, userId={}, operation={}, start", njId, userId, operation);
            try {
                long radioId = getRadioByUserId(njId);
                pushLogger.info("push hostSeatRole task for njId={}, userId={}, operation={}, radioId={}", njId, userId, operation, radioId);
                LZUserSyncPtlbuf.pushMyRole.Builder pushMyRoleBuilder = LZUserSyncPtlbuf.pushMyRole.newBuilder();

                LZModelsPtlbuf.roleInfo.Builder roleInfoBuilder = LZModelsPtlbuf.roleInfo.newBuilder();
                roleInfoBuilder.setRoleType(8);
                roleInfoBuilder.setOperation(operation);

                LZModelsPtlbuf.userRole.Builder userRoleBuilder = LZModelsPtlbuf.userRole.newBuilder();
                userRoleBuilder.addRoles(roleInfoBuilder);
                userRoleBuilder.setTargetId(radioId);
                userRoleBuilder.setTargetType(1);
                pushMyRoleBuilder.setUserRole(userRoleBuilder);

                int rCode;
                //if (liveAmusementConfig.isOpenDegrade()) {
                //    Result<Long> result = messagePushService.pushMessage2User(userId, LzProtocolCodec.generatePushMyRoleData(pushMyRoleBuilder.build()));
                //    rCode = result.rCode();
                //} else {
                //    Result<Void> result = messagePushApi.pushMessage2UserAsync(Arrays.asList(userId), LzProtocolCodec.generatePushMyRoleData(pushMyRoleBuilder.build()));
                //    rCode = result.rCode();
                //}
                Result<PushServiceProto.ResponsePushMessage2UserByte> result = messagePushService.pushMessage2UserByte(userId, LzProtocolCodec.generatePushMyRoleData(pushMyRoleBuilder.build()));
                rCode = result.rCode();
                pushLogger.info("push hostSeatRole task for njId={}, userId={}, operation={}, rCode={}, finish", njId, userId, operation, rCode);
            } catch (Exception e) {
                logger.error("push hostSeatRole occur ex for njId=" + njId + ", userId=" + userId + ", operation=" + operation, e);
            }
        }
    }

    /**
     * 根据用户ID获取radioId
     *
     * @param uid
     * @return
     */
    public long getRadioByUserId(long uid) {
        long radio = 0;
        try {
            Result<LiveRoomProto.ResponseGetLiveRoomByUserId> liveRoomByUserId = this.liveRoomService.getLiveRoomByUserId(uid);
            if (0 == liveRoomByUserId.rCode() && null != liveRoomByUserId.target()) {
                LiveRoomProto.LiveRoom liveRoom = liveRoomByUserId.target().getLiveRoom();
                radio = liveRoom.getRadioId();
            }
        } catch (Exception e) {
            logger.error("get radioId by userId error, uid:{}", uid, e);
            throw new RuntimeException(e);
        }
        return radio;
    }


    /**
     * <b><code>LiveFunWaitingUsersPushTask</code></b>
     * <p>
     * 直播娱乐模式排麦用户列表 liveFunWaitingUsers推送任务封装类
     * </p>
     * <b>Creation Time:</b> 2017年10月30日 下午4:43:22
     *
     * @author Zbean
     */
    private class WaitingUsersPushTask implements Runnable {
        //直播节目ID
        private long liveId;
        //主播ID
        private long njId;
        //轮播位上的主持
        private String hostSeatUserId;

        public WaitingUsersPushTask(long liveId, long njId, String hostSeatUserId) {
            this.liveId = liveId;
            this.njId = njId;
            this.hostSeatUserId = hostSeatUserId;
        }

        @Override
        public void run() {
            try {
                long nowTime = System.currentTimeMillis();
                LZModelsPtlbuf.liveFunWaitingUsers liveFunWaitingUsers = pushAdapterManager.buildLiveFunWaitingUsers(liveId, nowTime);
                LZUserSyncPtlbuf.pushLiveGeneralData.Builder pushLiveGeneralData = LZUserSyncPtlbuf.pushLiveGeneralData.newBuilder();
                pushLiveGeneralData.setType(2);
                pushLiveGeneralData.setLiveGeneralData(pushAdapterManager.buildLiveGeneralData(0, liveFunWaitingUsers.toByteString()));

                int rCode;
                if (liveAmusementConfig.isOpenDegrade()) {
                    Result<PushServiceProto.ResponsePushMessage2UserByte> result = messagePushService.pushMessage2UserByte(njId, pushAdapterManager.generatePushLiveGeneralData(pushLiveGeneralData.build()));
                    rCode = result.rCode();
                    pushLogger.info("push waitUser task for liveId:{}, njId:{}, pushTime:{}, rCode:{}", liveId, njId, nowTime, rCode);
                } else {
                    // 推送主播+主持
                    Set<Long> userIds = Sets.newHashSet();
                    userIds.add(njId);
                    if (!Strings.isNullOrEmpty(hostSeatUserId)
                            && Long.parseLong(hostSeatUserId) > SEAT_EMPTY && Long.parseLong(hostSeatUserId) != njId) {
                        userIds.add(Long.parseLong(hostSeatUserId));
                    }
                    userIds.addAll(amusementRedisManager.getRoomHostUsers(njId));

                    for (Long userId : userIds) {
                        pushLogger.info("push waitUser task for liveId:{}, userId={}", liveId, userId);
                        Result<PushServiceProto.ResponsePushMessage2UserByte> result = messagePushService.pushMessage2UserByte(userId, pushAdapterManager.generatePushLiveGeneralData(pushLiveGeneralData.build()));
                        rCode = result.rCode();
                        pushLogger.info("push waitUser task for liveId:{}, userId:{}, pushTime:{}, rCode:{}", liveId, userId, nowTime, rCode);
                    }
                    //Result<Void> result = messagePushApi.pushMessage2UserAsync(Lists.newArrayList(userIds), pushAdapterManager.generatePushLiveGeneralData(pushLiveGeneralData.build()));
                    //rCode = result.rCode();
                }
                //pushLogger.info("push waitUser task for liveId:{}, pushTime:{}, rCode:{}", liveId, nowTime, rCode);
            } catch (Exception e) {
                logger.error("push waitUser occur ex for liveId{}", liveId, e);
            }
        }
    }

    private class LiveRoomKickOutUserTask implements Runnable {

        private long liveId;
        private long userId;

        public LiveRoomKickOutUserTask(long liveId, long userId) {
            this.liveId = liveId;
            this.userId = userId;
        }
        @Override
        public void run() {
            try {
                LZModelsPtlbuf.liveUserKickedMsg msg = pushAdapterManager.buildLiveUserKickedMsg(liveId, userId);

                LZUserSyncPtlbuf.pushLiveGeneralData.Builder pushLiveGeneralData = LZUserSyncPtlbuf.pushLiveGeneralData.newBuilder();
                pushLiveGeneralData.setType(18);
                pushLiveGeneralData.setLiveGeneralData(pushAdapterManager.buildLiveGeneralData(0, msg.toByteString()));
                Result<PushServiceProto.ResponsePushMessage2UserByte> result = messagePushService.pushMessage2UserByte(userId, pushAdapterManager.generatePushLiveGeneralData(pushLiveGeneralData.build()));
                logger.info("pushMessage2User LiveRoomKickOutUserTask rCode={},liveId={},userId={}", result.rCode(), liveId, userId);
            } catch (Exception e) {
                logger.error("LiveRoomKickOutUserTask error liveId={},userId={},e={}", liveId, userId, e);
            }

        }
    }


    private class LiveCommentRevertTask implements Runnable {

        private long liveId;
        private long commentId;

        public LiveCommentRevertTask(long liveId, long commentId) {
            this.liveId = liveId;
            this.commentId = commentId;
        }
        @Override
        public void run() {
            try {
                LZModelsPtlbuf.liveComment msg = pushAdapterManager.buildLiveCommentMsg(commentId);

                LZUserSyncPtlbuf.pushLiveGeneralData.Builder pushLiveGeneralData = LZUserSyncPtlbuf.pushLiveGeneralData.newBuilder();
                pushLiveGeneralData.setType(23);
                pushLiveGeneralData.setLiveGeneralData(pushAdapterManager.buildLiveGeneralData(0, msg.toByteString()));

                Result<Void> result = studioLiveService.sendLivePushMsg(liveId, pushAdapterManager.generatePushLiveGeneralData(pushLiveGeneralData.build()));

                logger.info("LiveCommentRevertTask rCode={},liveId={},commentId={}", result.rCode(), liveId, commentId);
            } catch (Exception e) {
                logger.error("LiveCommentRevertTask error liveId={},commentId={},e={}", liveId, commentId, e);
            }

        }
    }

    private class PushTask<T extends Runnable> implements Delayed {

        //实际的任务
        private T t;

        //延迟到期时间
        private long expireTime;

        //任务序号
        private long serial;

        public PushTask(T t, long expireTime) {
            this.t = t;
            this.expireTime = expireTime;
            this.serial = atomic.incrementAndGet();
        }

        @Override
        public int compareTo(Delayed other) {
            if (other == this) {
                return 0;
            }
            if (other instanceof PushTask) {
                PushTask temp = (PushTask) other;
                long diff = expireTime - temp.expireTime;
                if (diff < 0)
                    return -1;
                else if (diff > 0)
                    return 1;
                else if (serial < temp.serial)
                    return -1;
                else
                    return 1;
            }
            long timeValue = (getDelay(TimeUnit.MILLISECONDS) - other.getDelay(TimeUnit.MILLISECONDS));
            return (timeValue == 0) ? 0 : ((timeValue < 0) ? -1 : 1);
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(this.expireTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 推送相亲房，特效相关
     * 特效类型：1 转场动画特效 2 牵手成功特效
     * @param liveId
     * @param effectType
     */
    public void pushAmusementBlindEffect(long liveId, int effectType) {
        executors.execute(() -> {
            try {
                PPliveBusiness.datingEffectPush push = PPliveBusiness.datingEffectPush.newBuilder()
                        .setEffectType(effectType)
                        .setLiveId(liveId)
                        .build();

                LZUserSyncPtlbuf.pushLiveGeneralData.Builder pushLiveGeneralData = LZUserSyncPtlbuf.pushLiveGeneralData.newBuilder();
                pushLiveGeneralData.setType(25);
                pushLiveGeneralData.setLiveGeneralData(pushAdapterManager.buildLiveGeneralData(0, push.toByteString()));

                Result<Void> result = studioLiveService.sendLivePushMsg(liveId, pushAdapterManager.generatePushLiveGeneralData(pushLiveGeneralData.build()));

                logger.info("pushAmusementBlindEffect rCode={},liveId={},effectType={}", result.rCode(), liveId, effectType);
            } catch (Exception e) {
                logger.error("pushAmusementBlindEffect error, liveId:{}, effectType:{}", liveId, effectType, e);
            }
        });
    }


    /**
     * 选择的用户下麦了推送
     * @param liveId
     * @param recUserId
     * @param leaveUserId
     */
    public void pushChoiceUserLevelMic(long liveId, long recUserId, long leaveUserId) {
        try {
            LZModelsPtlbuf.simpleUser simpleUser = pushAdapterManager.buildBlindRoomUser(leaveUserId);
            LZUserSyncPtlbuf.pushLiveGeneralData.Builder pushLiveGeneralData = LZUserSyncPtlbuf.pushLiveGeneralData.newBuilder();
            pushLiveGeneralData.setType(26);
            pushLiveGeneralData.setLiveGeneralData(pushAdapterManager.buildLiveGeneralData(0, simpleUser.toByteString()));

            Result<PushServiceProto.ResponsePushMessage2UserByte> result = messagePushService.pushMessage2UserByte(recUserId, pushAdapterManager.generatePushLiveGeneralData(pushLiveGeneralData.build()));
            logger.info("pushChoiceUserLevelMic rCode={},liveId={},userId={},leaveUserId={}", result.rCode(), liveId, recUserId, leaveUserId);
        } catch (Exception e) {
            logger.error("pushChoiceUserLevelMic error liveId={},userId={},leaveUserId={},e={}", liveId, recUserId, leaveUserId, e);
        }
    }

    /**
     * 当前轮结束了推送，客户端立即请求一次 相亲轮询
     */
    public void pushBlindEnd(long liveId) {
        try {
            // push只做占位作用，客户端只需要 type
            PPliveBusiness.datingEffectPush push = PPliveBusiness.datingEffectPush.newBuilder().build();

            LZUserSyncPtlbuf.pushLiveGeneralData.Builder pushLiveGeneralData = LZUserSyncPtlbuf.pushLiveGeneralData.newBuilder();
            pushLiveGeneralData.setType(27);
            pushLiveGeneralData.setLiveGeneralData(pushAdapterManager.buildLiveGeneralData(0, push.toByteString()));

            Result<Void> result = studioLiveService.sendLivePushMsg(liveId, pushAdapterManager.generatePushLiveGeneralData(pushLiveGeneralData.build()));

            logger.info("pushBlindEnd rCode={},liveId={}", result.rCode(), liveId);
        } catch (Exception e) {
            logger.error("pushBlindEnd error liveId={},e={}", liveId, e);
        }
    }

    /**
     * 推送直播间内印记连线
     */
    private void pushLiveBond(long liveId, boolean isBlindDate, List<LiveAmusementProto.LiveFunSeat> liveFunSeatsList) {
        try {
            LiveBondVO liveBondVO = bondManager.getUserBondByLive(liveId);
            if (liveBondVO == null) {
                return;
            }
            List<LiveMicBondVO> liveBond = liveBondVO.getLiveMicBondList();
            //注意这里区分null和empty判断：null表示因为没有人上麦从而没有必要推，empty表示目前没有印记
            if (liveBond == null) {
                return;
            }

            //印记的麦位更新为麦位轮询的
            int offset = isBlindDate ? -1 : 0;
            Map<Long, Integer> userId2SeatMap = liveFunSeatsList.stream().collect(Collectors.toMap(x -> x.getUserId(), x -> x.getSeat(), (a, b)->a));
            LizhiFMPtlbuf.structLiveBond.Builder liveBondBdr = LizhiFMPtlbuf.structLiveBond.newBuilder();
            for (LiveMicBondVO x : liveBond) {
                int seat1 = userId2SeatMap.getOrDefault(x.getUserId1(), -128) + offset;
                int seat2 = userId2SeatMap.getOrDefault(x.getUserId2(), -128) + offset;
                if (seat1 < 0 || seat2 < 0) {
                    continue;
                }
                liveBondBdr.addLiveMicBond(LizhiFMPtlbuf.liveMicBond.newBuilder()
                        .setIconUrl(x.getIconUrl())
                        .setUserId1(x.getUserId1())
                        .setUserId2(x.getUserId2())
                        .setSeat1(seat1)
                        .setSeat2(seat2));
            }

            //推送
            LZUserSyncPtlbuf.pushLiveGeneralData.Builder pushLiveGeneralData = LZUserSyncPtlbuf.pushLiveGeneralData.newBuilder();
            pushLiveGeneralData.setType(28);
            pushLiveGeneralData.setLiveGeneralData(pushAdapterManager.buildLiveGeneralData(0, liveBondBdr.build().toByteString()));
            Result<Void> res = studioLiveService.sendLivePushMsg(liveId, pushAdapterManager.generatePushLiveGeneralData(pushLiveGeneralData.build()));
            int rCode = res.rCode();
            if (!EnvUtils.isPro()) {
                logger.info("pushLiveBondLiveId={},rCode={}", liveId, rCode);
            }
        } catch (Exception e) {
            logger.warn("pushLiveBondErrorLiveId={}", liveId, e);
        }
    }


    /**
     * cp房推送倒计时时间-仅限于cp房业务使用
     * @param liveId
     * @param userId
     * @param countdown
     */
    public void pushCpRoomCountdown(long liveId, long userId, long countdown) {
        try {
            NewLiveAmusementProto.CpLiveInfo cpLiveInfo = NewLiveAmusementProto.CpLiveInfo.newBuilder().setCountdown(countdown).build();
            LZUserSyncPtlbuf.pushLiveGeneralData.Builder pushLiveGeneralData = LZUserSyncPtlbuf.pushLiveGeneralData.newBuilder();
            //cp房推送倒计时时间 type=31
            pushLiveGeneralData.setType(31);
            pushLiveGeneralData.setLiveGeneralData(pushAdapterManager.buildLiveGeneralData(0, cpLiveInfo.toByteString()));

            Result<Void> res = studioLiveService.sendLivePushMsg(liveId, pushAdapterManager.generatePushLiveGeneralData(pushLiveGeneralData.build()));
            logger.info("pushCpRoomCountdown rCode={},liveId={},userId={},countdown={}", res.rCode(), liveId, userId, countdown);
        } catch (Exception e) {
            logger.error("pushCpRoomCountdown error liveId={},userId={},countdown={},e={}", liveId, userId, countdown, e);
        }
    }

}
