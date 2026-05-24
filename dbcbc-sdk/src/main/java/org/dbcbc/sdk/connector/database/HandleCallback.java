/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbcbc.sdk.connector.database;

public interface HandleCallback {

    Object apply(DatabaseTemplate databaseTemplate) throws Exception;
}
