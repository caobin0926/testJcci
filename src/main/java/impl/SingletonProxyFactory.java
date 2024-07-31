package fm.lizhi.hy.vip.singleton.spring;

import fm.lizhi.common.dubbo.DubboClientBuilder;
import fm.lizhi.commons.config.service.ConfigService;
import fm.lizhi.commons.service.client.proxy.ProxyBuilder;
import fm.lizhi.commons.util.GuidGenerator;
import fm.lizhi.hy.idl.user.HyUserIDLService;
import fm.lizhi.hy.vip.config.VipConfig;
import fm.lizhi.live.amusement.utils.conf.UtilsConf;
import fm.lizhi.live.amusement.utils.other.lizhi.IdManager;
import fm.lizhi.live.amusement.utils.rpc.live.LiveManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.inject.Inject;

@Configuration
public class SingletonProxyFactory {

    @Inject
    private VipConfig vipConfig;

    @Inject
    private ProxyBuilder proxyBuilder;

    @Bean
    public GuidGenerator getGuidGenerator() {
        return new GuidGenerator(vipConfig.getServerId());
    }


    @Bean
    public UtilsConf getUtilsConf() {
        return ConfigService.loadConfig(UtilsConf.class, new String[]{"hy-amusement-utils"});
    }

    @Bean
    public IdManager getIaManager() {
        return new IdManager();
    }

    @Bean
    public LiveManager getLiveManager() {
        return new LiveManager();
    }


    @Bean
    public HyUserIDLService hyUserIDLService() {
        return new DubboClientBuilder<>(HyUserIDLService.class)
                .timeoutInMillis(5000)
                .sync()
                .retries(0)
                .build();
    }


}
