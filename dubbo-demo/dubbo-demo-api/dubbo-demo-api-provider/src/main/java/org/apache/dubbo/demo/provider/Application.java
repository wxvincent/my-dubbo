/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.demo.provider;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.MetadataReportConfig;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.ServiceConfig;
import org.apache.dubbo.config.bootstrap.DubboBootstrap;
import org.apache.dubbo.demo.DemoService;

import java.util.concurrent.CountDownLatch;

public class Application {
    public static void main(String[] args) throws Exception {
        if (isClassic(args)) {
            startWithExport();
        } else {
            startWithBootstrap();
        }
    }

    private static boolean isClassic(String[] args) {
        return args.length > 0 && "classic".equalsIgnoreCase(args[0]);
    }

    private static void startWithBootstrap() {
        ServiceConfig<DemoServiceImpl> service = new ServiceConfig<>();
        service.setInterface(DemoService.class);
        service.setRef(new DemoServiceImpl());

        DubboBootstrap bootstrap = DubboBootstrap.getInstance();
        bootstrap.application(new ApplicationConfig("dubbo-demo-api-provider"))
            .registry(new RegistryConfig("zookeeper://10.177.243.8:2181"))
            .protocol(new ProtocolConfig(CommonConstants.DUBBO, -1))
            .service(service)
            .start()
            .await();
    }

    // ?????????why????????????dubbo?????????????????????provider???consumer???????????????
    // provider???consumer????????????dubbo?????????????????????????????????????????????????????????

    // service???reference??????????????????????????????
    // ServiceInstance??????????????????????????????????????????????????????????????????????????????provider????????????????????????
    // ??????????????????????????????????????????consumer???????????????????????????
    // ??????????????????????????????????????????ServiceInstance??????????????????????????????????????????????????????????????????????????????
    // ???????????????ServiceInstance?????????????????????????????????????????????????????????????????????????????????????????????
    // ???????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????

    // dubbo????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
    // ?????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
    // ????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
    // ?????????????????????????????????????????????????????????

    // ServiceConfig -> ???????????????????????????????????????????????????
    // ReferenceConfig -> ???????????????????????????????????????????????????
    // ???????????????????????????dubbo???????????????????????????????????????????????????????????????????????????????????????????????????????????? -> ServiceConfig
    // ??????????????????????????????????????????????????????????????????????????????ReferenceConfig??????????????????ReferenceConfig???????????????????????????????????????????????????
    // ?????????????????????????????????????????????????????????XxConfig?????????

    private static void startWithExport() throws InterruptedException {
        // ?????????????????????????????????????????????ScopeModel???ApplicationModel??????????????????????????????????????????
        // ????????????????????????????????????????????????????????????ConfigManager???Repository
        // xxModel?????????????????????????????????????????????????????????????????????????????????????????????????????????
        // ??????????????????????????????????????????????????????????????????SPI??????????????????????????????SPI???????????????
        // ???????????????SPI???????????????????????????????????????????????????dubbo????????????????????????SPI????????????????????????SPI???????????????????????????

        //        ConfigCenterConfig configCenter = new ConfigCenterConfig();
        //        configCenter.setAddress("zookeeper://127.0.0.1:2181");

        // ServiceConfig????????????????????????
        // Service??????????????????????????????????????????????????????????????????????????????
        // ServiceConfig???????????????????????????dubbo???????????????????????????
        // ????????????????????????DemoServiceImpl?????????????????????????????????????????????????????????DemoServiceImpl=???????????????????????????
        ServiceConfig<DemoServiceImpl> service = new ServiceConfig<>();
        //        service.setConfigCenter(configCenter);
        // ???????????????????????????????????????
        service.setInterface(DemoService.class);
        // ???????????????????????????????????????????????????????????????
        service.setRef(new DemoServiceImpl());
        // application name??????????????????????????????????????????????????????
        service.setApplication(new ApplicationConfig("dubbo-demo-api-provider"));
        // ?????????rpc???????????????????????????????????????????????????????????????????????????????????????????????????
        // ???????????????????????????????????????????????????????????????????????????????????????
        // ??????????????????????????????????????????????????????????????????????????????????????????????????????????????????
        // ??????zookeeper???????????????????????????
        service.setRegistry(new RegistryConfig("zookeeper://10.177.243.8:2181"));
        // ????????????????????????dubbo???????????????????????????????????????????????????????????????????????????zk??????
        service.setMetadataReportConfig(new MetadataReportConfig("zookeeper://10.177.243.8:2181"));
        // ?????????????????????????????????export?????????????????????????????????????????????????????????????????????
        // ??????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????rpc??????
        // ??????????????????????????????????????????zk??????
        service.export();

        System.out.println("dubbo service started");
        new CountDownLatch(1).await();
    }
}
