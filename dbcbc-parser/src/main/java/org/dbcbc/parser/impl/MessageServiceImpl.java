package org.dbcbc.parser.impl;

import org.dbcbc.common.util.CollectionUtils;
import org.dbcbc.common.util.StringUtil;
import org.dbcbc.parser.MessageService;
import org.dbcbc.parser.ProfileComponent;
import org.dbcbc.parser.model.UserConfig;
import org.dbcbc.plugin.NotifyService;
import org.dbcbc.plugin.model.NotifyMessage;

import org.springframework.stereotype.Component;

import javax.annotation.Resource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Component
public class MessageServiceImpl implements MessageService {

    @Resource
    private NotifyService notifyService;

    @Resource
    private ProfileComponent profileComponent;

    @Override
    public void sendMessage(String title, String content) {
        UserConfig userConfig = profileComponent.getUserConfig();
        if (null == userConfig) {
            return;
        }

        List<String> mails = new ArrayList<>();
        userConfig.getUserInfoList().forEach(userInfo-> {
            if (StringUtil.isNotBlank(userInfo.getEmail())) {
                mails.addAll(Arrays.asList(StringUtil.split(userInfo.getEmail(), StringUtil.COMMA)));
            }
        });
        if (CollectionUtils.isEmpty(mails)) {
            return;
        }
        notifyService.sendMessage(NotifyMessage.newBuilder().setTitle(title).setContent(content).setReceivers(mails));
    }

}