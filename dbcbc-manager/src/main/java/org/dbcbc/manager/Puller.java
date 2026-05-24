package org.dbcbc.manager;

import org.dbcbc.parser.model.Mapping;

public interface Puller {

    void start(Mapping mapping);

    void close(String metaId);
}
