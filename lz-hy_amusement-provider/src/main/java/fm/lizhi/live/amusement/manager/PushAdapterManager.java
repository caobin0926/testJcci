package fm.lizhi.live.amusement.manager;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import com.netflix.governator.annotations.AutoBindSingleton;
import com.yibasan.lizhifm.protocol.LZModelsPtlbuf;
import com.yibasan.lizhifm.protocol.LZUserSyncPtlbuf;
import fm.lizhi.commons.config.util.JsonUtil;
import fm.lizhi.datacenter.common.protocol.CommProto;
import fm.lizhi.datacenter.radio.protocol.RadioProto;
import fm.lizhi.hy.amusement.enm.DressUpType;
import fm.lizhi.hy.amusement.protocol.DressUpInfoProto;
import fm.lizhi.live.amusement.bean.TeamMvpInfo;
import fm.lizhi.live.amusement.config.LiveAmusementConfig;
import fm.lizhi.live.amusement.constant.PhotoConstant;
import fm.lizhi.live.amusement.constant.RedisFieldConstant;
import fm.lizhi.live.amusement.constant.SeatStatus;
import fm.lizhi.live.amusement.dto.PushLiveFunDataDto;
import fm.lizhi.live.amusement.hy.api.HyNewLiveAmusementService;
import fm.lizhi.live.amusement.hy.protocol.LiveAmusementProto;
import fm.lizhi.live.amusement.local.cache.CacheLiveService;
import fm.lizhi.live.amusement.local.cache.CacheUserService;
import fm.lizhi.live.amusement.manager.blindroom.AmusementBlindManager;
import fm.lizhi.live.amusement.meta.bean.AmusementLevel;
import fm.lizhi.live.amusement.util.EnvUtils;
import fm.lizhi.live.amusement.util.GenerateIdUtil;
import fm.lizhi.live.amusement.util.ValueUtil;
import fm.lizhi.live.room.hy.constant.LayoutStatus;
import fm.lizhi.live.room.hy.constant.MicModeStatus;
import fm.lizhi.live.room.hy.enums.LiveRoomType;
import fm.lizhi.live.room.hy.protocol.LiveProto;
import fm.lizhi.live.room.hy.protocol.LiveRoomProto;
import fm.lizhi.pp.playgame.protocol.GameServiceProto;
import fm.lizhi.pp.util.constant.CdnConstant;
import fm.lizhi.server.business.util.ImgUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ObjectUtils;

import java.util.*;

import static fm.lizhi.live.amusement.constant.HostSeatConstants.*;

@AutoBindSingleton
public class PushAdapterManager {

    private static Logger logger = LoggerFactory.getLogger(PushAdapterManager.class);

    public static final int OP_CODE_PUSH_NET_SCENE_SYNC = 0x80 + 0x01;

    public static final int pushLiveGeneralDataCmd = 0xF025;

    // 1号麦位
    private static final int FIRST_SEAT_INDEX = 0;

    @Inject
    private AmusementLevelManager amusementLevelManager;

    @Inject
    private AmusementRedisManager amusementRedisManager;

    @Inject
    private LiveAmusementConfig liveAmusementConfig;

    @Inject
    private GiftRedisManager giftRedisManager;

    @Inject
    private SeatManager seatManager;

    @Inject
    private GeneralAdapterManager generalAdapterManager;

    @Inject
    private CacheLiveService cacheliveService;

    @Inject
    private CacheUserService cacheUserService;

    @Inject
    private LiveAmusementCarouselManager liveAmusementCarouselManager;

    @Inject
    private HostSeatRedisManager hostSeatRedisManager;

    @Inject
    private LiveAmusemenPlayGameManager liveAmusemenPlayGameManager;
    @Inject
    private LiveAmusementManager liveAmusementManager;
    @Inject
    private AgoraManager agoraManager;
    @Inject
    private UserTypeAuthenticationManager userTypeAuthenticationManager;
    @Inject
    private PpChannelLiveManager ppChannelLiveManager;
    @Inject
    private AmusementBlindManager amusementBlindManager;
    @Inject
    private UserManager userManager;
    @Inject
    private BondManager bondManager;
    @Inject
    private DressUpManager dressUpManager;

    /**
     * 把proto buffer结构体数组，编码成符合appserver通信协议的字节数组
     *
     * @param op   OP操作码
     * @param data protocol buffer的数组
     * @return
     */
    public static byte[] encode(int op, byte[] data) {
        ByteBuf buf = Unpooled.buffer(64);

        buf.writeInt(data.length + 9).writeByte(2).writeShort(0).writeShort(op).writeBytes(data);

        byte[] result = new byte[buf.readableBytes()];
        buf.readBytes(result);

        return result;
    }

    public byte[] generatePushLiveGeneralData(LZUserSyncPtlbuf.pushLiveGeneralData pushLiveGeneralData) {
        LZModelsPtlbuf.syncWrap.Builder syncWrapBuilder = LZModelsPtlbuf.syncWrap.newBuilder();
        syncWrapBuilder.setCmd(pushLiveGeneralDataCmd).setRawData(pushLiveGeneralData.toByteString());

        LZUserSyncPtlbuf.PushNetSceneSync pushNetSceneSync = LZUserSyncPtlbuf.PushNetSceneSync.newBuilder()
                .addSyncData(syncWrapBuilder).build();

        return encode(OP_CODE_PUSH_NET_SCENE_SYNC, pushNetSceneSync.toByteArray());
    }

    public LZModelsPtlbuf.liveGeneralData buildLiveGeneralData(int format, ByteString data) {
        LZModelsPtlbuf.liveGeneralData.Builder builder = LZModelsPtlbuf.liveGeneralData.newBuilder();
        builder.setFormat(format);
        builder.setData(data);
        return builder.build();
    }

    public PushLiveFunDataDto buildLiveFunData(long liveId, long nowTime) {
        LZModelsPtlbuf.liveFunData.Builder builder = LZModelsPtlbuf.liveFunData.newBuilder();
        Map<String, String> amusementBaseInfo = amusementRedisManager.getAmusementBaseInfo(liveId);
        Map<String, String> realTimeSeatInfo = seatManager.getAllSeatsMap(liveId);
        builder.setLiveId(liveId);
        builder.setTimestamp(nowTime);

        LiveProto.Live live = cacheliveService.getLive(liveId);
        //相亲房-获取房间类型
        int roomType = live.getRoomType();

        builder.setFunSwitch(buildLiveFunSwitch(liveId, nowTime, amusementBaseInfo, roomType));
        builder.setLikeMoment(buildLiveFunLikeMoment(liveId, nowTime, amusementBaseInfo));
        List<LZModelsPtlbuf.liveFunSeat> liveFunSeats = new ArrayList<>(12);
        List<LZModelsPtlbuf.liveFunSeat> extliveFunSeats = new ArrayList<>(12);
        LZModelsPtlbuf.liveFunSeat liveFunSeat = LZModelsPtlbuf.liveFunSeat.newBuilder().build();

        boolean isPlayGameRoom = liveAmusemenPlayGameManager.isPlayGameWithCache(liveId);
        LiveAmusementProto.ResponseLiveAmusementInfo responseLiveAmusementInfo = liveAmusementManager.liveAmusementInfo(liveId);
        List<LiveAmusementProto.LiveFunSeat> liveFunSeatsList = responseLiveAmusementInfo.getLiveFunSeatsList();
        List<GameServiceProto.UserGame> playGameGuestInfos;
        TeamMvpInfo teamMvpInfo = new TeamMvpInfo();
        LZModelsPtlbuf.liveFunTeamWar liveFunTeamWar = buildLiveFunTeamWar(liveId, amusementBaseInfo, realTimeSeatInfo, teamMvpInfo, roomType);


        long guestUserId;
        String value;

        // 9麦位布局 增加房主麦位
        long liveRoomId = live.getLiveRoomId();
        LiveRoomProto.LiveRoom liveRoom = cacheliveService.getLiveRoom(liveRoomId);
        int layout = liveAmusementManager.getliveRoomLayout(liveRoomId);
        int micMode = liveAmusementManager.getliveRoomMicMode(live.getLiveRoomId());
        long njId = live.getUserId();
        logger.info("buildLiveFunData extliveFunSeats liveId={}, layout={}, micMode={}", liveId, layout, micMode);

        int seatMax = seatManager.getSeatMax(liveId);
        boolean isExcludeExtraSeats = roomType != LiveRoomType.BLIND_DATE.getType() && roomType != LiveRoomType.SING.getType() && roomType != LiveRoomType.CP.getType() && roomType != LiveRoomType.PERSONAL.getType();
        if (LayoutStatus.nine.getValue() == layout && seatMax <= SeatStatus.SEAT_MAX && isExcludeExtraSeats) {
            liveFunSeat = buildLiveFunSeat(liveId, liveRoom.getUserId(), liveAmusementConfig.NJ_SEAT_NUM, teamMvpInfo, njId, roomType);
            extliveFunSeats.add(liveFunSeat);
            builder.addAllExtraSeats(extliveFunSeats);
            logger.info("buildLiveFunData extliveFunSeats liveId={}, seat={},charm={},speakState={},state={},uniqueId={}", liveId, liveFunSeat.getSeat(), liveFunSeat.getCharm(), liveFunSeat.getSpeakState(), liveFunSeat.getState(), liveFunSeat.getUniqueId());
        }

        //CP房-最大座位数 只有2个麦位 , 0号麦为房主，1号麦为嘉宾 , 0号麦开房后，房主就会自动上麦，所以只剩1个麦位
        if (roomType == LiveRoomType.CP.getType() && seatMax == SeatStatus.CP_ROOM_SEAT_MAX) {
            liveFunSeat = buildLiveFunSeat(liveId, njId, liveAmusementConfig.CP_ROOM_NJ_SEAT_NUM, teamMvpInfo, njId, roomType);
            extliveFunSeats.add(liveFunSeat);
            builder.addAllExtraSeats(extliveFunSeats);
            logger.info("buildLiveFunData cp extliveFunSeats liveId={}, seat={},charm={},speakState={},state={},uniqueId={}", liveId, liveFunSeat.getSeat(), liveFunSeat.getCharm(), liveFunSeat.getSpeakState(), liveFunSeat.getState(), liveFunSeat.getUniqueId());
        }

        //判断是否个播房和麦位数量
        //个播房-最大座位数-非白名单-房主会自动上了0号麦 - 有4个麦位
        //个播房-最大座位数-白名单用户，房主不用自动上0号麦 - 有5个麦位

        //个播房-最大座位数-非白名单-房主会自动上了0号麦 - 有4个麦位 , 则房主需要自己上麦到0号麦
//        if (roomType == LiveRoomType.PERSONAL.getType() && seatMax == SeatStatus.PERSONAL_ROOM_SEAT_MAX) {
//            liveFunSeat = buildLiveFunSeat(liveId, njId, 0, teamMvpInfo, njId, roomType);
//            extliveFunSeats.add(liveFunSeat);
//            builder.addAllExtraSeats(extliveFunSeats);
//            logger.info("buildLiveFunData personal extliveFunSeats liveId={}, seat={},charm={},speakState={},state={},uniqueId={}", liveId, liveFunSeat.getSeat(), liveFunSeat.getCharm(), liveFunSeat.getSpeakState(), liveFunSeat.getState(), liveFunSeat.getUniqueId());
//        }

        if (isPlayGameRoom) {
            playGameGuestInfos = getPlayGameGuestInfo(liveFunSeatsList, liveId);
            if (!liveFunSeatsList.isEmpty()) {
                int index = 0;
                for (LiveAmusementProto.LiveFunSeat funSeat : liveFunSeatsList) {
                    builder.addSeats(index, buildLiveFunSeatForGame(funSeat, playGameGuestInfos));
                    index++;
                }
            }
        } else {
            // 派对厅为0-7麦，相亲厅为0-8麦,getSeatMax对应多少麦位请参阅LiveAmusementManager.liveAmusementModeSwitch的seatManager.setSeatMax方法
            for (int seatNum = 0; seatNum < seatMax; seatNum++) {
                value = realTimeSeatInfo.get(String.valueOf(seatNum));
                if (value == null) {
                    guestUserId = 0;
                } else {
                    guestUserId = Long.valueOf(value);
                }

                //cp房-嘉宾位置固定为1号麦
                if (live.getRoomType() == LiveRoomType.CP.getType()) {
                    liveFunSeat = buildLiveFunSeat(liveId, guestUserId, 1, teamMvpInfo, njId, roomType);
                } else {
                    liveFunSeat = buildLiveFunSeat(liveId, guestUserId, seatNum, teamMvpInfo, njId, roomType);
                }
                liveFunSeats.add(liveFunSeat);
            }

            //相亲房 帽子数据处理
            if (roomType == LiveRoomType.BLIND_DATE.getType()) {
                liveFunSeats = liveAmusementManager.liveFunSeatCapLZModelsPtlbuf(liveId, liveFunSeats);
            }

            builder.addAllSeats(liveFunSeats);
        }

        // 交友房兼容魔法团战信息处理，这里用了新字段，为了兼容新旧版
        if (roomType == LiveRoomType.BLIND_DATE.getType() || roomType == LiveRoomType.SING.getType()) {
            builder.setTeamWarNew(liveFunTeamWar);
        } else {
            // 普通房团战信息
            builder.setTeamWar(liveFunTeamWar);
        }

        LZModelsPtlbuf.liveCarouselRoom liveCarouselRoom = buildCarouselRoom(liveId);
        if (liveCarouselRoom != null) {
            builder.setCarouselRoom(liveCarouselRoom);
        }

        //相亲房麦位模式+布局处理
        if (roomType == LiveRoomType.BLIND_DATE.getType()) {
            // 布局 1:8麦位布局  2:9麦位布局
            builder.setLayout(LayoutStatus.nine.getValue());
            // 麦位模式 1:申请上麦 2:自由上麦
            builder.setMicMode(MicModeStatus.apply_mode.getValue());
            LZModelsPtlbuf.liveFunSwitch funswitch = builder.getFunSwitch();
            if (!ObjectUtils.isEmpty(funswitch)) {
                funswitch = LZModelsPtlbuf.liveFunSwitch.newBuilder(funswitch).setFunModeType(6).setIsFunMode(true).build();
                builder.setFunSwitch(funswitch);
            }

        } else if (roomType == LiveRoomType.SING.getType()) {
            // 布局 1:8麦位布局  2:9麦位布局
            builder.setLayout(LayoutStatus.eight.getValue());
            // 麦位
            builder.setMicMode(micMode);
            LZModelsPtlbuf.liveFunSwitch funswitch = builder.getFunSwitch();
            if (!ObjectUtils.isEmpty(funswitch)) {
                funswitch = LZModelsPtlbuf.liveFunSwitch.newBuilder(funswitch).setFunModeType(7).setIsFunMode(true).build();
                builder.setFunSwitch(funswitch);
            }
        } else {
            //非相亲房麦位模式+布局处理 ， 原逻辑
            // 布局
            builder.setLayout(layout);
            // 麦位
            builder.setMicMode(micMode);
        }
        logger.info("buildLiveFunData liveId={}, layout={}, micMode={}", liveId, layout, micMode);

        //build
        return PushLiveFunDataDto.builder().liveFunData(builder.build())
                .blindDate(roomType == LiveRoomType.BLIND_DATE.getType())
                .liveFunSeats(liveFunSeatsList)
                .build();
    }

    private List<GameServiceProto.UserGame> getPlayGameGuestInfo(List<LiveAmusementProto.LiveFunSeat> liveFunSeatsList, Long liveId) {
        List<Long> users = Lists.newArrayList();
        for (LiveAmusementProto.LiveFunSeat item : liveFunSeatsList) {
            users.add(item.getUserId());
        }
        return liveAmusemenPlayGameManager.getPlayGameGuestInfo(users, liveId);
    }

    private LZModelsPtlbuf.liveFunTeamWar buildLiveFunTeamWar(long liveId, Map<String, String> amusementBaseInfo, Map<String, String> realTimeSeatInfo, TeamMvpInfo teamMvpInfo, int roomType) {
        long startTime = 0L, endTime = 0L;
        String value = amusementBaseInfo.get(RedisFieldConstant.TEAM_WAR_START_TIME);
        if (!Strings.isNullOrEmpty(value)) {
            startTime = Long.valueOf(value);
        }
        value = amusementBaseInfo.get(RedisFieldConstant.TEAM_WAR_END_TIME);
        if (!Strings.isNullOrEmpty(value)) {
            endTime = Long.valueOf(value);
        }
        LZModelsPtlbuf.liveFunTeamWar.Builder liveFunTeamWarBuilder = LZModelsPtlbuf.liveFunTeamWar.newBuilder();
        //startTime == 0表明之前没有开启过团战
        if (startTime == 0) {
            return liveFunTeamWarBuilder.build();
        }
        long nowTime = System.currentTimeMillis();
        //nowTime > endTime团战已经结束
        if (nowTime > endTime) {
            liveFunTeamWarBuilder.setState(2);
            liveFunTeamWarBuilder.setStartTime(startTime);
            return liveFunTeamWarBuilder.build();
        }
        //团战时间范围内
        if (nowTime >= startTime && nowTime <= endTime) {
            teamMvpInfo.setTeamWarMode(true);
            liveFunTeamWarBuilder.setState(1);
            liveFunTeamWarBuilder.setStartTime(startTime);
            liveFunTeamWarBuilder.setRemainingTime((endTime - nowTime) / 1000);
            LZModelsPtlbuf.liveFunTeamWarTeamInfo.Builder oneTeamBuilder = LZModelsPtlbuf.liveFunTeamWarTeamInfo.newBuilder();
            LZModelsPtlbuf.liveFunTeamWarTeamInfo.Builder twoTeamBuilder = LZModelsPtlbuf.liveFunTeamWarTeamInfo.newBuilder();
            Map<String, String> teamWarBaseInfo = amusementRedisManager.getTeamWarBaseInfo(liveId, startTime);
            int charm = 0;
            int level = 0;
            boolean hasCharm = false;
            AmusementLevel amusementLevel = null;

            value = teamWarBaseInfo.get(RedisFieldConstant.TEAM_CHARM_PREFIX + HyNewLiveAmusementService.TEAM_ONE);
            if (!Strings.isNullOrEmpty(value)) {
                hasCharm = true;
                charm = Integer.valueOf(value);
                oneTeamBuilder.setCharmValue(charm);
                level = amusementLevelManager.getAmusementLevle(charm);
                oneTeamBuilder.setTeamLevel(level);
            }
            amusementLevel = amusementLevelManager.getAmusementLevelCfg(level);
            if (amusementLevel == null) {
                amusementLevel = amusementLevelManager.getDefaultAmusementLevelCfg();
                logger.info("fail to get oneTeam amusementLevel for level:{}", level);
            }
            oneTeamBuilder.setNextFullCharm(amusementLevel.getUpperLimit());
            oneTeamBuilder.setCurrentBaseCharm(amusementLevel.getLowerLimit());

            value = teamWarBaseInfo.get(RedisFieldConstant.TEAM_CHARM_PREFIX + HyNewLiveAmusementService.TEAM_TWO);
            if (Strings.isNullOrEmpty(value)) {
                level = 0;
            } else {
                hasCharm = true;
                charm = Integer.valueOf(value);
                twoTeamBuilder.setCharmValue(charm);
                level = amusementLevelManager.getAmusementLevle(charm);
                twoTeamBuilder.setTeamLevel(level);
            }
            amusementLevel = amusementLevelManager.getAmusementLevelCfg(level);
            if (amusementLevel == null) {
                amusementLevel = amusementLevelManager.getDefaultAmusementLevelCfg();
                logger.info("fail to get twoTeam amusementLevel for level:{}", level);
            }
            twoTeamBuilder.setNextFullCharm(amusementLevel.getUpperLimit());
            twoTeamBuilder.setCurrentBaseCharm(amusementLevel.getLowerLimit());

            int[] seatNums = null;
            int mvpFlag = HyNewLiveAmusementService.TEAM_DOG_FALL;
            if (hasCharm) {
                seatNums = new int[]{0, 1, 2, 3, 4, 5, 6, 7};
                if (oneTeamBuilder.getCharmValue() > twoTeamBuilder.getCharmValue()) {
                    mvpFlag = HyNewLiveAmusementService.TEAM_ONE;
                } else if (oneTeamBuilder.getCharmValue() == twoTeamBuilder.getCharmValue()) {
                    mvpFlag = HyNewLiveAmusementService.TEAM_DOG_FALL;
                } else {
                    mvpFlag = HyNewLiveAmusementService.TEAM_TWO;
                }
            }

            liveFunTeamWarBuilder.setATeamInfo(oneTeamBuilder);
            liveFunTeamWarBuilder.setBTeamInfo(twoTeamBuilder);

            if (seatNums == null) {
                logger.info("hasCharm is false for liveId:{}", liveId);
                return liveFunTeamWarBuilder.build();
            }

            String userIdStr = null;
            int oneMaxCharm = -1, twoMaxCharm = -1, tempCharm = 0;
            long diffValue = (nowTime - startTime) / 1000;
            Map<Long, Integer> userCharms = teamMvpInfo.getUserCharms();
            boolean flag = diffValue >= liveAmusementConfig.getTeamWarDuration() ? true : false;
            int length = seatNums.length;
            for (int index = 0; index < length; index++) {
                userIdStr = realTimeSeatInfo.get(String.valueOf(seatNums[index]));
                if (Strings.isNullOrEmpty(userIdStr)) {
                    continue;
                }
                value = teamWarBaseInfo.get(userIdStr);
                if (Strings.isNullOrEmpty(value)) {
                    continue;
                }
                tempCharm = Integer.valueOf(value);
                userCharms.put(Long.valueOf(userIdStr), tempCharm);
                if (HyNewLiveAmusementService.TEAM_ONE == amusementLevelManager.getTeamSerial(seatNums[index], roomType)) {
                    if (tempCharm <= oneMaxCharm) {
                        continue;
                    }
                    oneMaxCharm = tempCharm;
                    if (flag && oneMaxCharm > liveAmusementConfig.getTeamMvpCharm()) {
                        if (mvpFlag == HyNewLiveAmusementService.TEAM_DOG_FALL || mvpFlag == HyNewLiveAmusementService.TEAM_ONE) {
                            teamMvpInfo.setTeamOneMvpUserId(Long.valueOf(userIdStr));
                        }
                    }
                } else if (HyNewLiveAmusementService.TEAM_TWO == amusementLevelManager.getTeamSerial(seatNums[index], roomType)) {
                    if (tempCharm <= twoMaxCharm) {
                        continue;
                    }
                    twoMaxCharm = tempCharm;
                    if (flag && twoMaxCharm > liveAmusementConfig.getTeamMvpCharm()) {
                        if (mvpFlag == HyNewLiveAmusementService.TEAM_DOG_FALL || mvpFlag == HyNewLiveAmusementService.TEAM_TWO) {
                            teamMvpInfo.setTeamTwoMvpUserId(Long.valueOf(userIdStr));
                        }
                    }
                } else {
                    logger.info("{} not in team for liveId:{}, seatNum:{}", userIdStr, liveId, seatNums[index]);
                }
            }
        }
        return liveFunTeamWarBuilder.build();
    }

    private LZModelsPtlbuf.liveCarouselRoom buildCarouselRoom(long liveId) {
        LZModelsPtlbuf.liveCarouselRoom.Builder builder = LZModelsPtlbuf.liveCarouselRoom.newBuilder();
        // 获取主播ID
        Long njId = liveAmusementCarouselManager.getNjIdWithCache(liveId);
        if (njId == null) {
            return null;
        }
        // 获取主持位信息
        long hostSeatUserId = Long.parseLong(hostSeatRedisManager.getHostSeatUserId(liveId));
        int hostSeatMikeState = Integer.parseInt(hostSeatRedisManager.getHostSeatMikeState(liveId));
        int hostSeatUniqueId = Integer.valueOf(hostSeatRedisManager.getHostSeatUniqueId(liveId));
        int terminalType = Integer.valueOf(hostSeatRedisManager.getHostSeatTerminalType(liveId));

        if (hostSeatUserId == SEAT_LOCK) {
            builder.setState(2);
        } else if (hostSeatUserId == SEAT_EMPTY) {
            builder.setState(1);
        } else if (hostSeatUserId > SEAT_EMPTY) {
            builder.setUserInfo(buildUserPlus(hostSeatUserId));
            if (hostSeatMikeState == MIKE_ON) {
                builder.setState(3);
            } else if (hostSeatMikeState == MIKE_OFF) {
                builder.setState(4);
            }
        }
        builder.setIsJockey(njId == hostSeatUserId);
        if (hostSeatUniqueId != NO_UNIQUE_ID) {
            builder.setUniqueId(hostSeatUniqueId);
        }

        if (terminalType != NO_TERMINAL_TYPE) {
            builder.setCallClient(terminalType);
        }

        return builder.build();
    }

    public LZModelsPtlbuf.userPlus buildUserPlus(long userId) {
        LiveRoomProto.LiveRoom liveRoom = cacheliveService.getLiveRoomByUserId(userId);
        RadioProto.Radio radio = cacheliveService.getRadio(liveRoom.getRadioId());

        LZModelsPtlbuf.userPlus.Builder builder = LZModelsPtlbuf.userPlus.newBuilder();
        builder.setUser(buildSimpleUser(userId));
        builder.setRadioId(radio.getId());
        builder.setWaveband(radio.getBand());
        return builder.build();
    }

    public LZModelsPtlbuf.simpleUser buildSimpleUser(long userId) {
        LZModelsPtlbuf.simpleUser.Builder builder = LZModelsPtlbuf.simpleUser.newBuilder();
        CommProto.User user = cacheUserService.getMinCacheUser(userId, null);
        if (user == null) {
            return builder.build();
        }
        builder.setUserId(userId);
        builder.setName(user.getName());
        builder.setGender(user.getGender());
        builder.setPortrait(
                buildPhoto(buildPortraitPath(user.getPortrait()), buildPortraitPath(user.getThumb()),
                        PhotoConstant.PORTRAIT_BIG_WIDTH, PhotoConstant.PORTRAIT_BIG_HEIGHT));;
        return builder.build();
    }

    public LZModelsPtlbuf.simpleUser buildBlindRoomUser(long userId) {
        LZModelsPtlbuf.simpleUser.Builder builder = LZModelsPtlbuf.simpleUser.newBuilder();
        Optional<CommProto.User> user = userManager.getUser(userId);
        builder.setUserId(user.map(CommProto.User::getId).orElse(0L));
//        builder.setName(user.map(CommProto.User::getName).orElse(""));
//        builder.setGender(user.map(CommProto.User::getGender).orElse(0));
//        builder.setPortrait(
//                buildPhoto(buildPortraitPath(user.map(CommProto.User::getPortrait).orElse("")),
//                        buildPortraitPath(user.map(CommProto.User::getThumb).orElse("")),
//                        PhotoConstant.PORTRAIT_BIG_WIDTH, PhotoConstant.PORTRAIT_BIG_HEIGHT));;
        return builder.build();
    }

    public LZModelsPtlbuf.photo buildPhoto(String cover, String thumb, int width, int height) {
        LZModelsPtlbuf.photo.Builder pBuilder = LZModelsPtlbuf.photo.newBuilder();
        pBuilder.setUrl(EnvUtils.isProduct() ? CdnConstant.cdnHostHYPro : CdnConstant.cdnHostTest);
        pBuilder.setOriginal(buildImage(cover, width, height));
        pBuilder.setThumb(buildImage(thumb, 0, 0));
        return pBuilder.build();
    }

    public LZModelsPtlbuf.image buildImage(String file, int width, int height) {
        LZModelsPtlbuf.image.Builder builder = LZModelsPtlbuf.image.newBuilder();
        if (file != null) {
            if (!file.startsWith("/")) {
                file = "/" + file;
            }
            builder.setFile(ImgUtil.getImageThumbUrl(file, width, height));
        }
        builder.setWidth(width);
        builder.setHeight(height);
        return builder.build();
    }


    public final String buildPortraitPath(String imgPath) {
        return imgPath == null ? null : buildUserPhotoPath(imgPath);
    }

    public final String buildUserPhotoPath(String imgPath) {
        return "/user/" + imgPath;
    }



    /**
     * 构造livePrompt消息体
     *
     * @param liveId:主播间ID
     * @param type:提示类型
     * @param msg:提示内容
     * @param action:Action 跳转
     * @param nowTime:时间戳
     * @return
     */
    public LZModelsPtlbuf.livePrompt buildLivePromt(long liveId, int type, String msg, String action, long nowTime) {
        LZModelsPtlbuf.livePrompt.Builder livePromptBuilder = LZModelsPtlbuf.livePrompt.newBuilder();
        livePromptBuilder.setLiveId(liveId);

        LZModelsPtlbuf.Prompt.Builder promptBuilder = LZModelsPtlbuf.Prompt.newBuilder();
        promptBuilder.setType(type);
        promptBuilder.setMsg(msg);
        if (null != action) {
            promptBuilder.setAction(action);
        }
        livePromptBuilder.setPrompt(promptBuilder);

        livePromptBuilder.setTimestamp(nowTime);
        return livePromptBuilder.build();
    }

    public LZModelsPtlbuf.liveFunSwitch buildLiveFunSwitch(long liveId, long nowTime, Map<String, String> amusementBaseInfo, int roomType) {
        LZModelsPtlbuf.liveFunSwitch.Builder builder = LZModelsPtlbuf.liveFunSwitch.newBuilder();
        String value = amusementBaseInfo.get(RedisFieldConstant.LIVE_AMUSEMENT_MODE);
        if (Strings.isNullOrEmpty(value)) {
            //没有值默认娱乐模式关闭
            builder.setIsFunMode(false);
        } else {
            int liveFunMode = Integer.valueOf(value);
            if (liveFunMode == HyNewLiveAmusementService.LIVE_AMUSEMENT_MODE_OPEN) {
                builder.setIsFunMode(true);
            } else {
                builder.setIsFunMode(false);
            }
        }
        builder.setCallChannel(buildCallChannel(amusementBaseInfo));
        value = amusementBaseInfo.get(RedisFieldConstant.UNIQUEID);
        if (!Strings.isNullOrEmpty(value)) {
            builder.setUniqueId(Integer.valueOf(value));
        }
        value = amusementBaseInfo.get(RedisFieldConstant.LIVE_AMUSEMENT_TYPE);
        if (!Strings.isNullOrEmpty(value)) {
            builder.setFunModeType(Integer.valueOf(value));
        }
        logger.debug("buildLiveFunSwitch liveId:{},roomType:{}", liveId, roomType);
        //设置相亲房funModeType=6
        if (roomType == LiveRoomType.BLIND_DATE.getType()) {
            builder.setFunModeType(6);
        }
        //点唱房-设置funModeType=7
        if (roomType == LiveRoomType.SING.getType()) {
            builder.setFunModeType(7);
        }
        //直播间房间类型是cp房则将FunModeType设置为8，8-表示cp房类型
        if(roomType == LiveRoomType.CP.getType()) {
            builder.setFunModeType(8);
        }
        //直播间房间类型是个播房则将FunModeType设置为9，9-表示个播房类型
        if(roomType == LiveRoomType.PERSONAL.getType()) {
            builder.setFunModeType(9);
        }

        // 轮播厅
//        if (liveAmusementCarouselManager.isCarouselWithCache(liveId)) {
//            builder.setFunModeType(2);
//        }
        // 约玩厅
//        if (liveAmusemenPlayGameManager.isPlayGameWithCache(liveId)) {
//            builder.setFunModeType(4);
//        }
        //游戏开黑房
//        if (liveAmusemenGameMatchRoomManager.isGameMatchRoomWithCache(liveId)) {
//            builder.setFunModeType(5);
//        }
        return builder.build();
    }

    public LZModelsPtlbuf.CallChannel buildCallChannel(Map<String, String> amusementBaseInfo) {
        LZModelsPtlbuf.CallChannel.Builder builder = LZModelsPtlbuf.CallChannel.newBuilder();
        String value = amusementBaseInfo.get(RedisFieldConstant.LIVE_AMUSEMENT_APP_KEY);
        if (!Strings.isNullOrEmpty(value)) {
            builder.setAppKey(value);
        }
        value = amusementBaseInfo.get(RedisFieldConstant.LIVE_AMUSEMENT_CHANNELID);
        if (!Strings.isNullOrEmpty(value)) {
            builder.setChannelId(value);
        }
        return builder.build();
    }

    public LZModelsPtlbuf.liveFunLikeMoment buildLiveFunLikeMoment(long liveId, long nowTime, Map<String, String> amusementBaseInfo) {
        LZModelsPtlbuf.liveFunLikeMoment.Builder builder = LZModelsPtlbuf.liveFunLikeMoment.newBuilder();
        String value = amusementBaseInfo.get(RedisFieldConstant.LIVE_LIKE_MOMENT_MODE);
        if (Strings.isNullOrEmpty(value)) {
            //没有值默认心动时刻未开始状态
            builder.setLikeMomentState(0);
        } else {
            builder.setLikeMomentState(Integer.valueOf(value));
        }
        value = amusementBaseInfo.get(RedisFieldConstant.LIVE_LIKE_MOMENT_START_TIME);
        if (!Strings.isNullOrEmpty(value)) {
            builder.setLikeMomentStartTime(Long.valueOf(value));
        }
        value = amusementBaseInfo.get(RedisFieldConstant.SNAPSHOOT_SEAT_INFO);
        if (!Strings.isNullOrEmpty(value)) {
            Map<String, String> likeMomentSeatInfo = JsonUtil.loads(value, Map.class);
            List<LZModelsPtlbuf.liveFunGuestLikeMoment> liveFunGuestLikeMoments = new ArrayList<>();
            LZModelsPtlbuf.liveFunGuestLikeMoment liveFunGuestLikeMoment;
            long guestUserId;
            int seatMax = seatManager.getSeatMax(liveId);
            // 派对厅为0-7麦，相亲厅为0-8麦,getSeatMax对应多少麦位请参阅LiveAmusementManager.liveAmusementModeSwitch的seatManager.setSeatMax方法
            for (int seatNum = 0; seatNum < seatMax; seatNum++) {
                value = likeMomentSeatInfo.get(String.valueOf(seatNum));
                if (Strings.isNullOrEmpty(value)) {
                    continue;
                }
                guestUserId = Long.valueOf(value);
                //座位上没人或是座位已被锁定
                if (guestUserId == 0 || guestUserId == -1) {
                    continue;
                }
                liveFunGuestLikeMoment = buildLiveFunGuestLikeMoment(liveId, guestUserId, seatNum, amusementBaseInfo);
                liveFunGuestLikeMoments.add(liveFunGuestLikeMoment);
            }
            builder.addAllLikeMomentResult(liveFunGuestLikeMoments);
        }
        return builder.build();
    }

    public LZModelsPtlbuf.liveFunGuestLikeMoment buildLiveFunGuestLikeMoment(long liveId, long guestUserId, int seat,
                                                                             Map<String, String> amusementBaseInfo) {
        LZModelsPtlbuf.liveFunGuestLikeMoment.Builder builder = LZModelsPtlbuf.liveFunGuestLikeMoment.newBuilder();
        Map<String, String> seatGuestInfo = amusementRedisManager.getSeatGuestInfo(liveId, guestUserId);
        builder.setUserId(guestUserId);
        String value = seatGuestInfo.get(RedisFieldConstant.SEAT_GUEST_LIKE_MOMENT_STATE);
        if (Strings.isNullOrEmpty(value)) {
            //没有值默认不在心动时刻
            builder.setUserLikeMomentState(0);
        } else {
            int userLikeMomentState = Integer.valueOf(value);
            builder.setUserLikeMomentState(userLikeMomentState);
            if (userLikeMomentState == 1) {
                int likeMomentMode;
                value = amusementBaseInfo.get(RedisFieldConstant.LIVE_LIKE_MOMENT_MODE);
                if (Strings.isNullOrEmpty(value)) {
                    likeMomentMode = HyNewLiveAmusementService.LIVE_LIKE_MOMENT_MODE_CLOSE;
                } else {
                    likeMomentMode = Integer.valueOf(value);
                }
                if (likeMomentMode == HyNewLiveAmusementService.LIVE_LIKE_MOMENT_MODE_CLOSE) {
                    builder.setUserLikeMomentState(3);
                }
            }
            //嘉宾已选择
            if (userLikeMomentState == 2) {
                value = seatGuestInfo.get(RedisFieldConstant.SEAT_SELECT_RESULT);
                if (!Strings.isNullOrEmpty(value)) {
                    builder.setSelectedUserId(Long.valueOf(value));
                }
            }
        }
        builder.setSeat(seat);
        return builder.build();
    }

    public LZModelsPtlbuf.liveFunSeat buildLiveFunSeat(long liveId, long guestUserId, int seat, TeamMvpInfo teamMvpInfo, long njId, int roomType) {
        LZModelsPtlbuf.liveFunSeat.Builder builder = LZModelsPtlbuf.liveFunSeat.newBuilder();
        builder.setSeat(seat);
        Map<String, String> seatGuestInfo = amusementRedisManager.getSeatGuestInfo(liveId, guestUserId);
        //座上有嘉宾才有 『是否离开』 的设置
        String isLeave = seatGuestInfo.get(RedisFieldConstant.SEAT_GUEST_STATE_IS_LEAVE);
        if(!Strings.isNullOrEmpty(isLeave)){
            logger.info("buildLiveFunSeat , liveId:{},guestUserId:{},isLeave:{}", liveId, guestUserId, isLeave);
            builder.setIsLeave(Integer.parseInt(isLeave));
        }
        //座位被锁定
        if (guestUserId == -1) {
            builder.setState(HyNewLiveAmusementService.SEAT_LOCK_STATE);
            //座位空闲
        } else if (guestUserId == 0) {
            builder.setState(HyNewLiveAmusementService.SEAT_FREE_STATE);
            //座位上有嘉宾
        } else {
            String value = seatGuestInfo.get(RedisFieldConstant.SEAT_GUEST_MICROPHONE_STATE);
            if (Strings.isNullOrEmpty(value)) {
                //没有值默认开麦中
                builder.setState(RedisFieldConstant.GUESET_MICROPHONE_OPEN_STATE);
            } else {
                builder.setState(Integer.valueOf(value));
            }
            builder.setUserId(guestUserId);
            builder.setCharm(giftRedisManager.getCharm(liveId, guestUserId));
            value = seatGuestInfo.get(RedisFieldConstant.SPEAK_STATE);
            int liveUserSpeakState;
            if (Strings.isNullOrEmpty(value)) {
                //没有值默认不在说话状态
                liveUserSpeakState = HyNewLiveAmusementService.SEAT_GUEST_SILENT_STATE;
            } else {
                liveUserSpeakState = Integer.valueOf(value);
                value = seatGuestInfo.get(RedisFieldConstant.SPEAK_STATE_MODIFY_TIME);
                if (!Strings.isNullOrEmpty(value)) {
                    long nowTime = System.currentTimeMillis();
                    long speakModifyTime = Long.valueOf(value);
                    if (nowTime - speakModifyTime > liveAmusementConfig.getUserStatusExpireSec() * 1000) {
                        //上次更新时间距当前时间超过60秒, 改为闭麦状态
                        liveUserSpeakState = HyNewLiveAmusementService.SEAT_GUEST_SILENT_STATE;
                    }
                }
            }
            builder.setSpeakState(liveUserSpeakState);
            value = seatGuestInfo.get(RedisFieldConstant.UNIQUEID);
            if (!Strings.isNullOrEmpty(value) && seat != liveAmusementConfig.NJ_SEAT_NUM) {
                builder.setUniqueId(Integer.valueOf(value));
            } else {
                int uid32 = ppChannelLiveManager.getUid32WithSwitch(guestUserId, 0, liveId);
                //相亲房的情况下，麦位是从0开始，所以不用做+1处理
                int seatNo = seat + 1;
                if (roomType == LiveRoomType.BLIND_DATE.getType()) {
                    seatNo = seat;
                }
                int uniqueId = uid32 > 0 ? uid32 : GenerateIdUtil.generateUniqueId(guestUserId, seatNo);
                agoraManager.agoraSync(liveId, guestUserId, seatNo);
                amusementRedisManager.updateGuestUniqueId(liveId, guestUserId, uniqueId);
                builder.setUniqueId(uniqueId);
            }
            value = seatGuestInfo.get(RedisFieldConstant.SWITCH_TERMINAL_TYPE);
            if (Strings.isNullOrEmpty(value)) {
                builder.setCallClient(HyNewLiveAmusementService.SWITCH_TERMINAL_TYPE_APP);
            } else {
                builder.setCallClient(Integer.valueOf(value));
            }
            //是否为主持
            builder.setRoomHost(generalAdapterManager.isRoomHostByNjId(njId, guestUserId));
//            builder.setRoomGuest(generalAdapterManager.isRoomAccByNjId(njId, guestUserId));
            builder.setRoomGuest(false);
            if (teamMvpInfo.isTeamWarMode()) {
                if (guestUserId == teamMvpInfo.getTeamOneMvpUserId() || guestUserId == teamMvpInfo.getTeamTwoMvpUserId()) {
                    builder.setTeamWarMvp(true);
                }
                Integer charm = teamMvpInfo.getUserCharms().get(guestUserId);
                if (charm != null) {
                    builder.setCharm(Integer.valueOf(charm));
                } else {
                    builder.setCharm(0);
                }
            }
        }
        // 所有麦位默认主持认证为false
        builder.setHostCertification(false);
        // 仅对一号麦位的用户进行主持认证的查询
        if (FIRST_SEAT_INDEX == seat) {
            Boolean hostCertification = userTypeAuthenticationManager.hostCertification(guestUserId);
            builder.setHostCertification(hostCertification);
        }

        //直播厅为相亲房
        if (roomType == LiveRoomType.BLIND_DATE.getType()) {
            logger.info("buildLiveFunSeat roomType:{},liveId:{},guestUserId:{}", roomType, liveId, guestUserId);
            LiveAmusementProto.LiveDatingSeat value = LiveAmusementProto.LiveDatingSeat.newBuilder().setChoiceUserId(0L).setPublishChoice(false).build();
            Double charm = null;

            if (guestUserId > 0) {
                value = amusementBlindManager.getLiveDatingSeat(liveId, guestUserId);
                charm = amusementBlindManager.getUserCharmDouble(liveId, guestUserId);
            }

            LZModelsPtlbuf.liveDatingSeat.Builder datingSeat = LZModelsPtlbuf.liveDatingSeat.newBuilder();
            datingSeat.setChoiceUserId(value.getChoiceUserId());
            datingSeat.setPublishChoice(value.getPublishChoice());
            datingSeat.setStatus(value.getStatus());
            builder.setLiveDatingSeat(datingSeat.build());
            //该魅力值返回给客户端，需取整
            builder.setCharm(charm == null ? 0 : ValueUtil.toInt(Math.round(charm)));
            //该魅力值有小尾巴，用于排序，用于相亲房
            builder.setLiveDatingCharm(charm == null ? 0 : charm);
        }

        DressUpInfoProto.DressUpInfo using = dressUpManager.getUserDressUsing(guestUserId, DressUpType.VOICE_WAVE.getId());
        if (using != null) {
            LZModelsPtlbuf.structLiveSoundWave.Builder liveSoundWaveBuilder = LZModelsPtlbuf.structLiveSoundWave.newBuilder();
            //声浪类型  1: 普通类型
            liveSoundWaveBuilder.setType(1);
            liveSoundWaveBuilder.setResourceUrl(using.getSvgaMaterialUrl());
            builder.setStructLiveSoundWave(liveSoundWaveBuilder.build());
        }
        return builder.build();
    }

    public LZModelsPtlbuf.liveFunWaitingUsers buildLiveFunWaitingUsers(long liveId, long nowTime) {
        LZModelsPtlbuf.liveFunWaitingUsers.Builder builder = LZModelsPtlbuf.liveFunWaitingUsers.newBuilder();
        builder.setLiveId(liveId);
        Set<String> waitUserIds = amusementRedisManager.getSeatQueueUsers(liveId);
        List<Long> userIds = new ArrayList<>();
        for (String waitUserId : waitUserIds) {
            userIds.add(Long.valueOf(waitUserId));
        }
        builder.addAllUserIds(userIds);
        builder.setTimestamp(nowTime);
        return builder.build();
    }

    /**
     * 直播娱乐模式座位 liveFunSeat
     *
     * @param liveFunSeat
     * @return
     */
    private LZModelsPtlbuf.liveFunSeat buildLiveFunSeatForGame(LiveAmusementProto.LiveFunSeat liveFunSeat,
                                                               List<GameServiceProto.UserGame> playGameGuestInfos) {
        LZModelsPtlbuf.liveFunSeat.Builder liveFunSeatBuilder = LZModelsPtlbuf.liveFunSeat.newBuilder();
        liveFunSeatBuilder.setSeat(liveFunSeat.getSeat());
        // 直播娱乐模式嘉宾魅力。 state=3或4时返回
        int state = liveFunSeat.getState();
        liveFunSeatBuilder.setState(state);
        if(liveFunSeat.hasIsLeave()){
            liveFunSeatBuilder.setIsLeave(liveFunSeat.getIsLeave());
        }
        if (state == 3 || state == 4) {
            // 座位上的用户ID。state=3或4时返回
            liveFunSeatBuilder.setUserId(liveFunSeat.getUserId());
            // 用户的魅力值 。state=3或4时返回
            liveFunSeatBuilder.setCharm((int) liveFunSeat.getLiveFunGuestCharm());
            // 用户说话状态。state=3或4时返回
            liveFunSeatBuilder.setSpeakState(liveFunSeat.getSpeakState());
            // 用户的唯一标识。state=3或4时返回
            liveFunSeatBuilder.setUniqueId(liveFunSeat.getUniqueId());
            // 用户是否为主持。state=3或4时返回
            liveFunSeatBuilder.setRoomHost(liveFunSeat.getRoomHost());
            // 通话终端。state=3或4时返回
            liveFunSeatBuilder.setCallClient(liveFunSeat.getTerminalType());
            // 是否团战的MVP
            liveFunSeatBuilder.setTeamWarMvp(liveFunSeat.getTeamWarMvp());
            // 所有麦位默认主持认证为false
            liveFunSeatBuilder.setHostCertification(false);
            // 仅对一号麦位的用户进行主持认证的查询
            if (FIRST_SEAT_INDEX == liveFunSeat.getSeat()) {
                Boolean hostCertification = userTypeAuthenticationManager.hostCertification(liveFunSeat.getUserId());
                liveFunSeatBuilder.setHostCertification(hostCertification);
            }
        }

        for (GameServiceProto.UserGame item : playGameGuestInfos) {
            if (item.getUserId() == liveFunSeat.getUserId()
                    && item.getUserId() > 0) {
                //额外信息
                liveFunSeatBuilder.setPlayGameSeat(LZModelsPtlbuf.playGameSeat.newBuilder()
                        .setGameArea(item.getGame().getAreas(0).getName())
                        .setGameName(item.getGame().getName())
                        .setJoinNum(item.getRequestNum())
                        .setValidTime(liveFunSeat.getLastModifyTime() + liveAmusementConfig.getSecondExpired() * 1000)
//                            .setIsJoin(this.isJoin(item.getLiveId(),currentUserId,item.getUserId()))
                        .build());
                break;
            }
        }

        return liveFunSeatBuilder.build();
    }

    public LZModelsPtlbuf.liveUserKickedMsg buildLiveUserKickedMsg(long liveId, long userId) {
        return LZModelsPtlbuf.liveUserKickedMsg.newBuilder()
                .setLiveId(liveId)
                .setUserId(userId)
                .build();
    }

    public LZModelsPtlbuf.liveComment buildLiveCommentMsg(long commentId) {
        return LZModelsPtlbuf.liveComment.newBuilder()
                .setId(commentId)
                .build();
    }
}
