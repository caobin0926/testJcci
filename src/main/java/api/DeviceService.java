package api;

import java.util.List;

/**
 * @author caobin
 */
public interface DeviceService {

    /**
     * 查询设备记录
     * @param id 设备记录ID
     * @return 设备记录
     */
    String selectHeiyeDeviceById(Long id);

    /**
     * 查询设备记录列表
     * @param heiyeDevice 设备记录
     * @return 返回设备记录列表
     */
    List<String> selectHeiyeDeviceList(String heiyeDevice);

    /**
     * 新增设备记录
     * @param heiyeDevice 设备记录
     * @return 结果
     */
    int saveHeiyeDevice(String heiyeDevice);

    /**
     *
     * 修改设备号
     * @param heiyeDevice 设备记录
     * @return 结果
     */
    int updateHeiyeDevice(String heiyeDevice);
}
