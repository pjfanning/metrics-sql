package com.github.gquintana.metrics.util;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

public class DefaultObjectNameFactory implements ObjectNameFactory {

    public ObjectName createName(String type, String domain, String name) {
        try {
            ObjectName objectName = new ObjectName(domain, "name", name);
            if (objectName.isPattern()) {
                objectName = new ObjectName(domain, "name", ObjectName.quote(name));
            }

            return objectName;
        } catch (MalformedObjectNameException mone) {
            try {
                return new ObjectName(domain, "name", ObjectName.quote(name));
            } catch (MalformedObjectNameException e) {
                //LOGGER.warn("Unable to register {} {}", new Object[]{type, name, e});
                throw new RuntimeException(e);
            }
        }
    }
}