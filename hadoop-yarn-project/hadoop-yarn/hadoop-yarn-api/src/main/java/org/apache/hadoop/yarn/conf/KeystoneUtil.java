package org.apache.hadoop.yarn.conf;

import org.apache.hadoop.conf.Configuration;

/**
 * Created by zhuochen on 17/8/21.
 */
public class KeystoneUtil {

    public static boolean isKeystoneEnabled(Configuration conf) {
        return conf.getBoolean(YarnConfiguration.RM_KEYSTONE_ENABLED,
                YarnConfiguration.DEFAULT_RM_KEYSTONE_ENABLED);
    }

    public static String[] initConf(Configuration conf) {
        return new String[] {
            conf.get(YarnConfiguration.RM_KEYSTONE_URL),
            conf.get(YarnConfiguration.RM_KEYSTONE_REGION),
            conf.get(YarnConfiguration.RM_KEYSTONE_USERNAME),
            conf.get(YarnConfiguration.RM_KEYSTONE_PASSWORD)
        };
    }
}
