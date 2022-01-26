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
package org.apache.dubbo.rpc.proxy;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.profiler.Profiler;
import org.apache.dubbo.common.profiler.ProfilerEntry;
import org.apache.dubbo.common.profiler.ProfilerSwitch;
import org.apache.dubbo.rpc.Constants;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.RpcServiceContext;
import org.apache.dubbo.rpc.model.ConsumerModel;
import org.apache.dubbo.rpc.model.ServiceModel;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Map;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_TIMEOUT;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;

/**
 * InvokerHandler
 */
public class InvokerInvocationHandler implements InvocationHandler {
    private static final Logger logger = LoggerFactory.getLogger(InvokerInvocationHandler.class);
    private final Invoker<?> invoker;
    private ServiceModel serviceModel;
    private URL url;
    private String protocolServiceKey;

    public InvokerInvocationHandler(Invoker<?> handler) {
        this.invoker = handler;
        this.url = invoker.getUrl();
        this.protocolServiceKey = this.url.getProtocolServiceKey();
        this.serviceModel = this.url.getServiceModel();
    }

    // 通过这个代理对象，就可以拿到这个代理对象的接口，通过接口就可以定位到目标服务实例发布的服务接口
    // 针对目标服务实例进行调用，方法和参数是谁，调用目标服务实例的哪个方法，传递进去哪些参数
    // 已经可以拿到完整的rpc调用需要的一些信息了
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        // InvokerInvocationHandler.invoke
        // 动态代理，就是这个意思，就是针对接口动态的生成这个接口的实现类
        // 比如说针对DemoService这个接口去生成一个实现类，但是这个实现类是动态生成的，他怎么知道，如果你调用他的方法，要如何执行呢？
        // 动态代理底层都是要封装InvocationHandler，对动态代理的所有方法的调用，都会走到InvocationHandler这里来
        // 他的invoke方法，就可以拿到说，proxy是动态代理的对象，调用的是哪个方法Method，方法里传入的是什么参数，args
        // 当我知道了这些事情之后，对动态代理的不同的方法的调用，具体方法被调用之后，逻辑是如何来处理
        if (method.getDeclaringClass() == Object.class) {
            return method.invoke(invoker, args);
        }
        String methodName = method.getName();
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length == 0) {
            if ("toString".equals(methodName)) {
                return invoker.toString();
            } else if ("$destroy".equals(methodName)) {
                invoker.destroy();
                return null;
            } else if ("hashCode".equals(methodName)) {
                return invoker.hashCode();
            }
        } else if (parameterTypes.length == 1 && "equals".equals(methodName)) {
            return invoker.equals(args[0]);
        }
        // consumer这端，rpc调用，必须封装一个RpcInvocation
        // 会传递到provider这一端，那边拿到你的请求数据之后，也会搞出来一个RpcInvocation
        RpcInvocation rpcInvocation = new RpcInvocation(serviceModel, method, invoker.getInterface().getName(), protocolServiceKey, args);
        String serviceKey = url.getServiceKey();
         // 调用的目标服务实例他的service name
        rpcInvocation.setTargetServiceUniqueName(serviceKey);

        // invoker.getUrl() returns consumer url.
        RpcServiceContext.setRpcContext(url);

        if (serviceModel instanceof ConsumerModel) {
            rpcInvocation.put(Constants.CONSUMER_MODEL, serviceModel);
            rpcInvocation.put(Constants.METHOD_MODEL, ((ConsumerModel) serviceModel).getMethodModel(method));
        }

        if (ProfilerSwitch.isEnableSimpleProfiler()) {
            ProfilerEntry parentProfiler = Profiler.getBizProfiler();
            ProfilerEntry bizProfiler;
            boolean containsBizProfiler = false;
            if (parentProfiler != null) {
                containsBizProfiler = true;
                bizProfiler = Profiler.enter(parentProfiler, "Receive request. Client invoke begin.");
            } else {
                bizProfiler = Profiler.start("Receive request. Client invoke begin.");
            }
            rpcInvocation.put(Profiler.PROFILER_KEY, bizProfiler);
            try {
                return invoker.invoke(rpcInvocation).recreate();
            } finally {
                Profiler.release(bizProfiler);
                if (!containsBizProfiler) {
                    int timeout;
                    Object timeoutKey = rpcInvocation.getObjectAttachmentWithoutConvert(TIMEOUT_KEY);
                    if (timeoutKey instanceof Integer) {
                        timeout = (Integer) timeoutKey;
                    } else {
                        timeout = url.getMethodPositiveParameter(methodName, TIMEOUT_KEY, DEFAULT_TIMEOUT);
                    }
                    long usage = bizProfiler.getEndTime() - bizProfiler.getStartTime();
                    if ((usage / (1000_000L * ProfilerSwitch.getWarnPercent())) > timeout) {
                        StringBuilder attachment = new StringBuilder();
                        for (Map.Entry<String, Object> entry : rpcInvocation.getObjectAttachments().entrySet()) {
                            attachment.append(entry.getKey()).append("=").append(entry.getValue()).append(";\n");
                        }

                        logger.warn(String.format("[Dubbo-Consumer] execute service %s#%s cost %d.%06d ms, this invocation almost (maybe already) timeout\n" +
                                "invocation context:\n%s" +
                                "thread info: \n%s",
                            protocolServiceKey, methodName, usage / 1000_000, usage % 1000_000,
                            attachment, Profiler.buildDetail(bizProfiler)));
                    }
                }
            }
        }

        return invoker.invoke(rpcInvocation).recreate();
    }
}
