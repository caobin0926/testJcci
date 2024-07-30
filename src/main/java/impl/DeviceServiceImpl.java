package impl;

import api.DeviceService;

import java.util.List;

/**
 * Created on 2024/07/30
 *
 * @author 你的名字
 */

public class DeviceServiceImpl implements DeviceService {

    @Override
    public String selectHeiyeDeviceById(Long id) {
        System.out.println("selectHeiyeDeviceById");
        return null;
    }

    @Override
    public List<String> selectHeiyeDeviceList(String heiyeDevice) {
        System.out.println("selectHeiyeDeviceById");
        return null;
    }

    @Override
    public int saveHeiyeDevice(String heiyeDevice) {
        return 1;
    }

    @Override
    public int updateHeiyeDevice(String heiyeDevice) {
        return 1;
    }

}
