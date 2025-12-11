package ai.basic.x1.adapter.api.job;

import ai.basic.x1.adapter.api.context.RequestContext;
import ai.basic.x1.adapter.api.context.RequestContextHolder;
import ai.basic.x1.adapter.api.context.UserInfo;
import ai.basic.x1.entity.ModelMessageBO;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamListener;

import java.util.concurrent.ConcurrentHashMap;

import java.util.List;
/**
 * @author andy
 */
@Slf4j
public class DatasetModelJobConsumerListener implements StreamListener<String, ObjectRecord<String, String>> {

    private String group;
    private String streamKey;
    private RedisTemplate<String, Object> redisTemplate;
    private ConcurrentHashMap<String, AbstractModelMessageHandler> modelMessageHandlerMap = new ConcurrentHashMap<>();

    public DatasetModelJobConsumerListener(String streamKey, String group, RedisTemplate<String, Object> redisTemplate, ApplicationContext applicationContext) {
        this.streamKey = streamKey;
        this.group = group;
        this.redisTemplate = redisTemplate;
        for (AbstractModelMessageHandler messageHandler : applicationContext.getBeansOfType(AbstractModelMessageHandler.class).values()) {
            modelMessageHandlerMap.put(messageHandler.getModelCodeEnum().name(), messageHandler);
        }
    }


    @Override
    public void onMessage(ObjectRecord message) {
        String jsonStr = (String) message.getValue();
        try {
            if (jsonStr.trim().startsWith("[")) {
                // JSON 배열 → List<ModelMessageBO>
                List<ModelMessageBO> modelMessageList = JSONUtil.toList(jsonStr, ModelMessageBO.class);
                // log.info("modelMessageList : {}", modelMessageList);
                // log.info("modelMessageList size: {}", modelMessageList.size());
    
                if (!modelMessageList.isEmpty()) {
                    // User context는 첫 번째 메시지 기준으로 세팅
                    buildRequestContext(modelMessageList.get(0).getCreatedBy());
    
                    // 배치 전체를 handler로 넘김
                    AbstractModelMessageHandler handler =
                            modelMessageHandlerMap.get(modelMessageList.get(0).getModelCode().name());
    
                    if (handler != null && handler.handleDatasetModelRunBatch(modelMessageList)) {
                        redisTemplate.opsForStream().acknowledge(streamKey, group, message.getId());
                    }
                }
    
            } else {
                // 단일 메시지 처리 (기존 로직)
                ModelMessageBO modelMessageBO = JSONUtil.toBean(jsonStr, ModelMessageBO.class);
                buildRequestContext(modelMessageBO.getCreatedBy());
                AbstractModelMessageHandler handler =
                        modelMessageHandlerMap.get(modelMessageBO.getModelCode().name());
                if (handler != null && handler.handleDatasetModelRun(modelMessageBO)) {
                    redisTemplate.opsForStream().acknowledge(streamKey, group, message.getId());
                }
            }
    
        } catch (Exception e) {
            log.error("메시지 처리 중 오류", e);
        }
    }

    private void buildRequestContext(Long userId) {
        RequestContext requestContext = RequestContextHolder.createEmptyContent();
        requestContext.setUserInfo(UserInfo.builder().id(userId).build());
        RequestContextHolder.setContext(requestContext);
    }
}
