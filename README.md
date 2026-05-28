

> 特点
* 组合驱动，自定义库同步到库组合，关系型数据库与非关系型之间组合，任意搭配表同步映射关系
* 实时监控，驱动全量或增量实时同步运行状态、结果、同步日志和系统日志
* 开发插件，自定义转化同步逻辑

## 近期更新（2.0.0）

### 连接管理

| 功能 | 说明 |
|------|------|
| **连接错误提示增强** | 保存/测试连接失败时，给出可读中文原因（如端口不通、主机不可达、用户名密码错误、库不存在等），不再仅显示 JDBC 原始异常 |
| **保存重试与超时** | 添加/编辑/复制连接时最多重试 **3 次**、总耗时 **30 秒**；超时或连不上仍保存配置，错误写入 `connectionError` |
| **连接错误按钮** | 列表「操作」列在 **复制** 与 **删除** 之间增加按钮，点击可查看最近一次保存时的完整连接错误 |
| **失败写入性能监控** | 新增/修改/复制/测试连接失败时，除「新增连接器」等操作日志外，会在 **性能监控 → 日志** 中额外记录「连接失败」明细 |
| **刷新连接状态** | 列表页增加 **刷新** 按钮，重新检测所有连接在线状态；新增连接后若仍显示离线，可点此刷新 |
| **添加页类型精简** | 「添加连接」类型下拉 **仅显示**：DM、MySQL、PostgreSQL、Oracle、SqlServer、File |

**添加连接时已隐藏的类型**（已有连接不受影响，仍可正常使用）：Kafka、Elasticsearch、Http、SQLite 等。

### 数据验证

| 功能 | 说明 |
|------|------|
| **四种验证策略** | 行数对比、行级对比（全量签名比较）、校验和（MD5）、分层校验（三层逐级深入） |
| **分层校验** | 行数对比 → 分 chunk 校验和 → 异常 chunk 行级采样，漏斗式缩小排查范围 |
| **异构数据库支持** | 签名计算在 Java 层完成（排序 + MD5），支持 MySQL、Oracle、PostgreSQL、达梦、SQL Server 互验 |
| **结果持久化** | 验证结果自动写入 `dbcbc_task_data_verification_detail` 表，页面刷新不丢失 |
| **历史记录** | 详情页支持查看历次验证结果、查看详情（JSON 反序列化渲染）、删除记录 |
| **Oracle CLOB 兼容** | 分层校验 SQL 使用 `DBMS_LOB.SUBSTR` 替代 `TO_CHAR`，避免 ORA-22835 错误 |

### DDL 结构迁移

| 功能 | 说明 |
|------|------|
| **两阶段迁移** | ① **表结构迁移**（建表/补字段/索引）→ ② **数据库对象迁移**（过程/函数/视图等）；阶段一完成后可继续阶段二，也可仅做表结构 |
| **索引同步（CREATE/DROP INDEX）** | 表结构迁移时自动同步二级索引：源端新增索引 → 目标端自动创建；源端删除索引 → 目标端自动删除。支持 MySQL、PostgreSQL、Oracle、达梦、SQL Server |
| **SQL Server → PostgreSQL** | 新增表结构迁移链路（类型映射 `sqlserver2pg`）及对象迁移（视图、序列、存储过程、函数、触发器等 T-SQL→PL/pgSQL 最大努力转换） |
| **Oracle → 达梦（DM）表结构** | 新增 `oracle2dm` 类型映射与 `ORACLE2DM` 转换器；Oracle 类型（`VARCHAR2`、`NUMBER`、`TIMESTAMP`、`CLOB` 等）按达梦兼容类型转换，未映射类型保留源端精度 |
| **Oracle 对象迁移** | 阶段二支持 Oracle → PostgreSQL / 达梦 / MySQL，迁移存储过程、函数、触发器、包、视图、同义词、序列等 |
| **迁移路径提示** | 选择源/目标连接后，页面显示当前路径及是否支持对象迁移 |
| **迁移日志着色** | ERROR 红色，WARN 蓝色，SUCCESS 绿色 |
| **表结构完成统计** | 阶段一结束后展示：源库对象总数（表、索引）、目标库对象总数（表、索引） |
| **对象迁移统计** | 阶段二结束后展示：成功 / 警告 / 失败 / 跳过 / 合计 |
| **人工核查提示** | 跨库过程/函数/触发器转换后默认 WARN；Job、权限/角色类对象不自动执行，仅写入日志需人工处理 |
| **达梦新建用户口令** | Oracle→达梦迁移时，若目标需新建与源同名的 schema 用户，默认口令为 `{用户名}_@123`（达梦不允许口令与登录名相同） |

### 数据验证

数据验证模块（侧栏 **「数据验证」**）用于对比源表与目标表的数据一致性，支持对 **增量同步驱动** 执行四种验证策略。

#### 验证策略

| 策略 | 说明 |
|------|------|
| **行数对比** | 对比源表和目标表的行数是否一致，快速发现数据缺失 |
| **行级对比** | 全量行级对比：逐行签名比较，定位缺失/多余/不一致行并输出字段级差异 |
| **校验和** | 对比源表和目标表的 MD5 校验和（基于排序后的行签名），快速检测数据差异 |
| **分层校验** | 三层逐级深入：① 行数对比 → ② 分 chunk 校验和 → ③ 异常 chunk 内行级采样；前一层不通过则终止后续层 |

#### 分层校验详解

分层校验采用漏斗式策略，逐层缩小排查范围：

1. **第一层（COUNT）**：对比源/目标行数；不一致则反查缺失主键（最多显示 20 条），终止后续层
2. **第二层（Checksum）**：按主键范围分 chunk（每 chunk 10000 行），逐 chunk 计算签名校验和；全部一致则通过，否则标记异常 chunk
3. **第三层（Sampling）**：仅在异常 chunk 内做精确行级比对，输出字段级差异报告（最多显示 20 条）

#### 结果持久化

验证结果自动写入数据库表 `dbcbc_task_data_verification_detail`，页面刷新后不丢失。支持在详情页查看 **历史记录**，包括查看详情和删除操作。

> 验证结果以 JSON 格式存储在 `CONTENT` 字段中，每条记录对应一张表的一次验证结果。

#### 验证结果存储表（MySQL）

验证结果持久化使用 MySQL 存储，表结构如下：

```sql
CREATE TABLE `dbcbc_task_data_verification_detail` (
    `ID` varchar(64) NOT NULL COMMENT '唯一ID',
    `TASK_ID` varchar(64) NOT NULL COMMENT '关联的驱动ID（mappingId）',
    `TYPE` varchar(32) NOT NULL COMMENT '验证类型: count / sampling / checksum / cascade',
    `SOURCE_TABLE_NAME` varchar(100) DEFAULT '' COMMENT '源表名称',
    `TARGET_TABLE_NAME` varchar(100) DEFAULT '' COMMENT '目标表名称',
    `CONTENT` text NOT NULL COMMENT '验证结果JSON',
    `CREATE_TIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `UPDATE_TIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`ID`),
    KEY `IDX_UPDATE_CREATE_TIME` (`UPDATE_TIME`,`TYPE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据校验任务明细表';
```

#### 存储连接配置

验证结果表由系统自动创建，存储后端通过 `application.properties` 配置：

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `dbcbc.storage.type` | 存储类型：`disk`（Lucene 本地文件）或 `mysql` | `disk` |
| `dbcbc.storage.mysql.url` | MySQL JDBC 连接地址 | `jdbc:mysql://127.0.0.1:3306/dbcbc?...` |
| `dbcbc.storage.mysql.username` | MySQL 用户名 | `root` |
| `dbcbc.storage.mysql.password` | MySQL 密码 | `123` |
| `dbcbc.storage.mysql.driver-class-name` | JDBC 驱动类名 | （可选，自动检测） |

**使用 MySQL 存储验证结果的配置步骤**：

1. 确保 MySQL 实例可访问，建议创建专用数据库（默认 `dbcbc`）
2. 修改 `conf/application.properties`，将 `dbcbc.storage.type` 改为 `mysql`：
   ```properties
   dbcbc.storage.type=mysql
   dbcbc.storage.mysql.url=jdbc:mysql://your-mysql-host:3306/dbcbc?rewriteBatchedStatements=true&characterEncoding=UTF8&serverTimezone=Asia/Shanghai
   dbcbc.storage.mysql.username=your_user
   dbcbc.storage.mysql.password=your_password
   ```
3. 重启服务，系统自动创建 `dbcbc_task_data_verification_detail` 表（及其他系统表）

> **注意**：当 `dbcbc.storage.type=disk` 时，验证结果存储在本地 Lucene 索引（`data/` 目录）中，不依赖 MySQL。两种模式功能一致，但 MySQL 模式便于运维管理和数据迁移。

#### 使用说明

1. 在 **驱动管理** 中配置增量同步驱动（源 → 目标）
2. 打开 **数据验证**，选择目标驱动
3. 选择验证策略，点击 **「开始验证」**
4. 验证过程中可实时查看进度和结果
5. 验证完成后，点击 **「历史记录」** 查看历次验证结果

---

### 达梦（DM）增量同步 → PostgreSQL

| 功能 | 说明 |
|------|------|
| **日志增量** | 支持达梦作为源、PostgreSQL 作为目标的 **日志增量** 同步（基于 DM `DBMS_LOGMNR` 归档挖掘） |
| **前置条件** | 达梦需开启归档（`ARCH_INI=1`）、逻辑日志（`RLOG_APPEND_LOGIC=1/2/3/4`），同步账号需 DBA 或 LogMiner 相关权限 |
| **配置方式** | 连接管理配置 DM 源 + PG 目标 → 驱动管理选择 **增量**、策略 **日志** → 配置表映射后启动 |

> 说明：定时增量（timing）策略对达梦原本可用；本次主要补齐 **日志增量**（log）。跨库 DDL 自动同步仍不支持，仅同步 DML 变更。

### 错误重试

| 功能 | 说明 |
|------|------|
| **增量同步启动重试** | 增量同步驱动启动失败时自动重试最多 3 次（间隔 5s / 10s / 20s），避免瞬时网络异常导致同步永久终止 |
| **全量同步表级重试** | 全量同步中单表失败时自动重试该表最多 3 次，失败后跳过继续下一张表，不再中断整个同步流程 |
| **增量写入重试** | 增量数据写入目标库失败时自动重试最多 3 次，避免瞬时数据库异常导致数据静默丢失 |

### 批量操作

| 功能 | 说明 |
|------|------|
| **驱动批量启动** | 驱动列表支持复选框多选，一键批量启动多个驱动，逐个执行并返回成功/失败统计 |
| **驱动批量停止** | 驱动列表支持复选框多选，一键批量停止多个运行中的驱动 |
| **表组批量添加** | 驱动编辑页支持多选源表和目标表，一次添加多组映射关系（已有功能） |
| **表组批量删除** | 驱动编辑页表组列表支持复选框全选/多选，一键删除多个表组（已有功能） |

### API 文档（Swagger/OpenAPI）

| 功能 | 说明 |
|------|------|
| **OpenAPI 3.0 规范** | 自动生成 JSON 格式的 OpenAPI 规范文件，地址：`/v3/api-docs` |
| **Swagger UI** | 交互式 API 文档界面，地址：`/swagger-ui.html`，支持在线测试接口 |
| **接口覆盖** | 自动扫描所有 Controller 端点：连接管理、驱动管理、表组管理、DDL 迁移、数据验证、监控等 |
| **无侵入** | 基于现有 `@RequestMapping`/`@PostMapping`/`@GetMapping` 注解自动生成，无需修改已有代码 |

**访问方式**：启动服务后，浏览器打开 `http://127.0.0.1:18686/swagger-ui.html`

### 同步延迟监控

| 功能 | 说明 |
|------|------|
| **实时延迟展示** | 驱动列表新增"同步延迟"列，实时展示每个驱动距上次同步批次的时间差 |
| **颜色标识** | 绿色（< 30秒）：同步正常；黄色（30秒~5分钟）：存在延迟；红色（> 5分钟）：延迟较高 |
| **自动刷新** | 驱动列表每 5 秒自动刷新，延迟数据实时更新 |
| **未运行驱动** | 停止状态的驱动显示为灰色 "--" |

### 操作审计日志

| 功能 | 说明 |
|------|------|
| **操作人记录** | 所有日志自动记录操作人用户名（从 Spring Security 获取），非登录操作记录为 `system` |
| **关键操作覆盖** | 连接器/驱动/表组的增删改查、驱动启停、DDL 迁移（表结构+对象）、数据验证、系统配置修改、RSA/API 密钥生成、配置导入导出等均有日志记录 |
| **日志查询** | 监控页面日志表格支持按关键字搜索、展示操作人列、相对时间显示 |

> 项目地址

## 🌈应用场景
| 连接器        | 数据源 | 目标源 | 支持版本(包含以下)    | 连接管理「添加」页 |
|------------|---|---|---------------|----------------|
| MySQL      | ✔ |  ✔ | 5.7.19以上      | ✔ 显示 |
| Oracle     | ✔ |  ✔ | 10g-19c       | ✔ 显示 |
| SqlServer  | ✔ |  ✔ | 2008以上        | ✔ 显示 |
| PostgreSQL | ✔ |  ✔ | 9.5.25以上      | ✔ 显示 |
| DM（达梦）   | ✔ |  ✔ | DM8（日志增量需归档+LogMiner） | ✔ 显示 |
| File       | ✔ |  ✔ | *.txt, *.unl  | ✔ 显示 |
| Sqlite     | ✔ |  ✔ | 2以上           | 隐藏 |
| ES         | ✔ |  ✔ | 6.0.0-8.15.3  | 隐藏 |
| Kafka      | ✔ |  ✔ | 2.10-0.9.0.0以上 | 隐藏 |
| Http       | ✔ |  ✔ |   | 隐藏 |
| SQL        | ✔ |  | 支持以上关系型数据库    | — |
| 后期计划       | Redis | |               | — |

## DDL 迁移（表结构 + 数据库对象）

在 Web 侧提供 **「DDL迁移」** 入口（侧栏），用于在已配置的 **关系型数据库连接** 之间做两阶段迁移（以连接管理中的连接器为源与目标）：

- **阶段一**：表结构迁移（建表/补字段/索引同步，不迁移业务数据）
- **阶段二**：数据库对象迁移（存储过程、函数、触发器、视图、序列等）

**支持路径**：Oracle → PostgreSQL / 达梦 / MySQL、**SQL Server → PostgreSQL**。

### 索引同步（CREATE/DROP INDEX）

阶段一表结构迁移时，自动同步源表与目标表的二级索引（非主键索引）：

| 场景 | 行为 | 支持的数据库 |
|------|------|-------------|
| 源端新增索引，目标端不存在 | 自动 **CREATE INDEX** | MySQL、PostgreSQL、Oracle、达梦、SqlServer |
| 源端删除索引，目标端仍存在 | 自动 **DROP INDEX** | MySQL、PostgreSQL、Oracle、达梦、SqlServer |
| 源端索引已存在，目标端也存在 | 跳过，不做修改 | — |

**支持的索引类型**：普通索引、唯一索引、全文索引（FULLTEXT）、空间索引（SPATIAL）、位图索引（BITMAP，Oracle/达梦）、GIN/GiST/BRIN/Hash（PostgreSQL）。

**注意事项**：
- 索引同步仅在阶段一（表结构迁移）时执行，与建表/补字段同一流程
- Oracle 函数索引、表达式索引等含不可迁移列的索引会跳过并记录 WARN 日志
- 达梦会自动过滤 Oracle 系统列（`sys_nc\d+$`）相关的索引列

迁移过程可在页面查看执行日志；**ERROR 为红色、WARN 为蓝色、SUCCESS 为绿色**；阶段一完成后显示 **源库/目标库对象总数（表、索引）**；阶段二完成后显示 **成功/警告/失败/跳过/合计** 统计。

### 使用说明（简要）

1. 在 **连接管理** 中配置好源库、目标库连接器。
2. 打开 **DDL迁移**，选择源/目标连接，点击 **「开始迁移表结构」** 执行阶段一。
3. 表结构迁移完成后，按需勾选对象类型，点击 **「开始迁移对象」** 执行阶段二（也可跳过，仅做表结构）。
4. 迁移完成后，若目标侧 **新建了数据库**，连接器里缓存的库名列表可能仍是旧的，请在 DDL 页面使用 **「刷新连接库列表」**（或等价操作），再在驱动/映射里选择新库。

### 注意事项（必读）

#### 1. PostgreSQL：界面上的「目标库」与 JDBC 实际连接的库必须一致

驱动（映射）配置里的 **目标库**（`targetDatabase`，例如 `monitor`）仅表示业务上选中的库名；**真正连上 PostgreSQL 并拉取表清单时，使用的是「目标连接器」里配置的 JDBC URL 中的库名**（例如 `jdbc:postgresql://host:5432/postgres` 则始终连的是 `postgres`）。

当前实现会在连接上尝试 `setCatalog(目标库)`，但 **PostgreSQL JDBC 不能像 MySQL 那样靠 `setCatalog` 可靠地切换到另一个物理库**。若 URL 仍指向 `postgres`，则 `DatabaseMetaData.getTables` 枚举的是 **postgres** 里的表，与你在 `monitor` 里 `\d` 看到的表不一致。

**现象**：目标库显示 `monitor`，目标表下拉却只有 **postgres** 里那几张表；DDL 已迁到 `monitor` 的几十张表始终不出现。

**处理**：

- 将 **目标连接器** 的 JDBC URL 中的库名改为实际要同步的库（例如 `.../monitor`），保存后，在驱动编辑页对目标再点 **「刷新表」**；或  
- 新建一个 **JDBC 已指向目标库** 的连接器，驱动里改用该连接器作为目标。

#### 2. 驱动管理：目标表下拉数据从哪来、何时刷新

- 目标表选项来自驱动配置里 **已缓存的 `targetTable` 列表**，不是每次打开页面都实时扫库。
- 修改连接器 URL、或确认目标库已变更后，请在驱动编辑页点击 **「刷新表」**，否则列表可能仍是旧库的表。
- 默认会 **隐藏已在「表映射关系」里使用过的目标表**，避免重复选择；若需要看到全部表，使用多选里的 **「取消过滤」**（带 `exclude=1` 刷新页面）。这与「库连错」不同：取消过滤只解决过滤，不能解决 URL 仍指向错误库的问题。

#### 3. DDL 与类型映射（Oracle / SQL Server → PostgreSQL / 达梦 等）

- 结构迁移模块内对类型映射做了增强，例如 Oracle 中带精度的 `TIMESTAMP(n)`、SQL Server 类型等与未映射类型会按约定落到目标库可执行类型（PostgreSQL 如 `TEXT`，达梦保留 Oracle 兼容类型），避免生成 `"列名" null not null` 等非法 DDL。
- **Oracle → 达梦**：使用 `TypeMapping.json` 中 `oracle2dm` 映射；目标库不存在同名用户时会尝试 `CREATE USER` 并自动切换到新用户 schema 建表。
- 阶段二的过程/函数/触发器为 **最大努力语法替换**，建议人工核查后再上线；Job、权限/角色仅输出说明，需手动在目标库执行。

#### 4. Oracle → 达梦（DM）补充说明

- 达梦 **禁止口令与登录名相同**，新建用户（如 `MONITOR`）时默认口令为 **`MONITOR_@123`**；若用户已存在则跳过建用户，直接在现有 schema 中建表。
- 若迁移日志仍出现类型相关 ERROR，请确认已重新编译部署含 `oracle2dm` 映射的版本，并以页面红色日志为准排查特殊类型。

#### 5. 操作建议小结

| 场景 | 建议 |
|------|------|
| DDL 在目标实例上 **新建了 database** | DDL 页刷新连接库列表；必要时重选连接器缓存的库列表 |
| 映射里目标库与 `psql` 里不一致 | 检查连接器 JDBC URL 库名是否与 `targetDatabase` 一致（PostgreSQL 尤其重要） |
| 目标表太少、但库里明明很多表 | 先「取消过滤」排除映射过滤；再「刷新表」；仍不对则检查 JDBC URL |
| Oracle→达梦建用户失败 | 检查是否口令策略冲突；用户已存在时可忽略，确认表是否建到正确 schema |
| 迁移报错 | 查看 DDL 迁移页日志中的红色错误信息 |

> **补充**：DDL 相关接口：`POST /ddl/transfer`（阶段一：表结构迁移）；`POST /ddl/transferObjects`（阶段二：对象迁移，参数 `objectTypes` 如 `PROCEDURE,FUNCTION,VIEW`）；`POST /ddl/refreshConnectorMeta`（刷新连接器缓存的库列表，与 DDL 页 **「刷新连接库列表」** 按钮一致）。

---

## 安装部署

### 方式一：二进制安装包（dbcbc-2.0.0-bin.zip）

使用 **`dbcbc-2.0.0-bin.zip`** 在本地安装并启动 DBcbc（数据库同步服务）。

#### 环境要求

| 项目 | 说明 |
|------|------|
| **操作系统** | Linux x86_64 / aarch64、macOS（与启动脚本支持一致）；Windows 可使用 `bin` 目录下的对应脚本 |
| **JDK** | **JDK 1.8**（路径需加入环境变量，或在 `bin/startup.sh` 中指定 `JAVA_HOME`） |
| **内存** | 默认 JVM 约 **3GB** 堆（`-Xms3g` / `-Xmx3g`），机器物理内存建议不少于 **4GB** |
| **磁盘** | 预留日志与数据目录空间（解压目录下 `logs`、`data` 等会随运行增长） |

#### 解压安装

1. 将 **`dbcbc-2.0.0-bin.zip`** 复制到目标目录，例如 Linux：`/opt/dbcbc`，Windows：`D:\Programs\dbcbc`。
2. **解压** ZIP，得到安装根目录（通常名为 `dbcbc-2.0.0`，以实际解压结果为准）。
3. 确认目录结构包含：`bin/`（启动/停止脚本）、`conf/`（如 `application.properties`）、`lib/`（程序与依赖 Jar）。

> 下文 **安装根目录** 均指解压后的顶层目录。

#### 配置（可选）

按需修改 **`conf/application.properties`**，常见项：

- **`server.port`**：Web 管理服务端口（默认 **`18686`**）
- **`server.ip`**（若配置）：监听地址

修改前建议备份配置文件。Linux / macOS 若未全局配置 JDK，可在 **`bin/startup.sh`** 中取消注释并设置：

```bash
#JAVA_HOME=/opt/jdk1.8.0_202
```

#### 启动与校验

**Linux / macOS：**

```bash
cd 安装根目录/bin
chmod +x startup.sh stop.sh    # 若无可执行权限时执行一次
./startup.sh
```

看到 **`Start successfully! PID: xxxx`** 表示启动成功。

**Windows：** 进入 **`安装根目录\bin`**，执行 `startup.bat` 或 `startup.cmd`（若存在）。

**访问 Web：** http://127.0.0.1:18686（若改了端口则用新端口）。默认账号 **admin** / **admin**，首次登录后建议修改密码。

#### 停止服务

```bash
cd 安装根目录/bin
./stop.sh
```

Windows 请使用包内停止脚本或按运维规范结束主类 **`org.dbcbc.web.Application`** 对应进程。

#### 目录与日志

| 路径（相对安装根目录） | 用途 |
|------------------------|------|
| `logs/` | 运行日志、错误日志等 |
| `conf/` | 配置文件 |
| `lib/` | 程序库 |
| `tmp.pid` | 启动脚本可能写入的进程 PID（以实际为准） |

启动失败请先查看 **`logs`** 下最新日志；OOM 时可能生成 **`logs/heapdump.hprof`**。

#### 端口与防火墙

默认 Web 端口 **`18686`**。远程访问时请在防火墙或云安全组中 **放行对应 TCP 端口**。

#### 安装常见问题

1. **提示找不到 `JAVA_HOME` 或 `java`**：安装 JDK 8 并配置 `PATH` / `JAVA_HOME`，或在 **`startup.sh`** 中指定 `JAVA_HOME`。
2. **端口被占用**：修改 **`conf/application.properties`** 中的 **`server.port`**，或关闭占用端口的进程后重启。
3. **内存不足**：在 **`bin/startup.sh`** 中适当调小 `-Xms` / `-Xmx`（需与机器内存匹配，过小可能影响大表同步）。
4. **MySQL 5.x 连接问题**：可能需要替换兼容的 MySQL 驱动 Jar 到 `lib` 目录，见下文「连接与同步 FAQ」。

也可从 [Releases](https://gitee.com/ghi/dbcbc/releases) 下载安装包，或按下方「手动编译」自行构建。

### 方式二：Docker
* 阿里云镜像
```shell
# 社区版
docker pull scxhtb-registry.cn-hangzhou.cr.aliyuncs.com/xhtb/dbcbc:latest
docker pull scxhtb-registry.cn-hangzhou.cr.aliyuncs.com/xhtb/dbcbc:2.0.8
# 专业版
docker pull scxhtb-registry.cn-hangzhou.cr.aliyuncs.com/xhtb/dbcbc-enterprise:latest
docker pull scxhtb-registry.cn-hangzhou.cr.aliyuncs.com/xhtb/dbcbc-enterprise:2.0.8
```
* 运行命令
```shell
docker run -d \
  --name=dbcbc \
  --restart=unless-stopped \
  -p 18686:18686 \
  -e TZ="Asia/Shanghai" \
  -m 5g \
  --memory-swap=5g \
  -v /opt/dbcbc/data:/app/dbcbc/data \
  -v /opt/dbcbc/logs:/app/dbcbc/logs \
  -v /opt/dbcbc/plugins:/app/dbcbc/plugins \
  --log-driver json-file \
  --log-opt max-size=100m \
  --log-opt max-file=7 \
  scxhtb-registry.cn-hangzhou.cr.aliyuncs.com/xhtb/dbcbc:latest

# 本地日志
ls -la /opt/dbcbc/logs

# 容器日志
docker logs --tail 20 dbcbc
# 容器实时日志（Ctrl+C退出）
docker logs -f dbcbc
# 进入容器内部
docker exec -it dbcbc /bin/bash
# 查看容器日志
ls -la /app/dbcbc/logs

# 停止容器
docker stop dbcbc
# 启动容器
docker start dbcbc
# 重启容器
docker restart dbcbc
# 删除容器（需先停止）
docker rm dbcbc
```

---

## Oracle 源库增量同步注意事项（如 Oracle → PostgreSQL / MySQL / 达梦）

以下面向 **Oracle 作为源、开启增量（LogMiner）** 的场景。Oracle 端通过 **`DBMS_LOGMNR`** 从 redo / 归档解析变更，**断点续传位点为 SCN**；目标库通过 JDBC 写入。全量同步仍以 Oracle 连接器正常读表为准。

### 实例与库级配置

| 项目 | 说明 |
|------|------|
| **归档模式** | 持续增量一般需要 **`ARCHIVELOG`**（`SELECT LOG_MODE FROM V$DATABASE` 可查）。`NOARCHIVELOG` 下通常无法满足持续增量。 |
| **补充日志** | 重放 **`UPDATE`/`DELETE`** 等往往需要 **supplemental logging**（库级或表级 **主键补充日志**）。缺失时易出现无法唯一定位行、解析不完整等问题。 |
| **网络与连接** | 放行监听端口；`SERVICE_NAME`/SID 与 JDBC 一致；**RAC/PDB/CDB** 时连接串、会话容器需与实际部署一致。 |
| **与目标库差异** | 字符集、时区、`TIMESTAMP WITH TIME ZONE`、大字段与类型映射等可能影响落地，见上文 DDL / 驱动管理相关说明。 |
| **版本支持** | 连接器标注 **Oracle 10g–19c**；**11g** 与 **12c+** 权限要求不同（见下表）。 |

### 同步用 Oracle 账号权限（程序启动时会校验）

满足 **其一** 即可在「角色」侧快捷通过：**拥有 `DBA` 角色**则不再逐项检查；否则 **必须** 授予 **`SELECT_CATALOG_ROLE`**。

此外，当前会话需具备以下 **系统权限**（在 `SESSION_PRIVS` 中均可看到）：

| Oracle 大版本 | 所需权限 |
|---------------|----------|
| **11g 及以下** | `CREATE SESSION`、`SELECT ANY TRANSACTION`、`SELECT ANY DICTIONARY` |
| **12c 及以上** | 上述三项 + **`LOGMINING`** |

由管理员执行时可参考（将 `your_sync_user` 换成实际用户；**11g 不要** 授权 `LOGMINING`）。

**Oracle 11g 及以下**（无 `LOGMINING`，须显式授权 `DBMS_LOGMNR` 包）：

```sql
GRANT SELECT_CATALOG_ROLE TO your_sync_user;
GRANT CREATE SESSION TO your_sync_user;
GRANT SELECT ANY TRANSACTION TO your_sync_user;
GRANT SELECT ANY DICTIONARY TO your_sync_user;
GRANT EXECUTE ON SYS.DBMS_LOGMNR TO your_sync_user;
GRANT EXECUTE ON SYS.DBMS_LOGMNR_D TO your_sync_user;
GRANT EXECUTE ON SYS.DBMS_FLASHBACK TO your_sync_user;
```

**Oracle 12c 及以上**：

```sql
GRANT SELECT_CATALOG_ROLE TO your_sync_user;
GRANT CREATE SESSION TO your_sync_user;
GRANT SELECT ANY TRANSACTION TO your_sync_user;
GRANT SELECT ANY DICTIONARY TO your_sync_user;
GRANT LOGMINING TO your_sync_user;
-- 若仍报 PLS-00201，再补：
GRANT EXECUTE ON SYS.DBMS_LOGMNR TO your_sync_user;
GRANT EXECUTE ON SYS.DBMS_LOGMNR_D TO your_sync_user;
GRANT EXECUTE ON SYS.DBMS_FLASHBACK TO your_sync_user;
```

若还需 **全量、对账** 等读表操作，须对涉及 **业务表/schema** 授予 **`SELECT`**（或通过角色集中授权）。

获取 SCN 时若无法访问 `V$DATABASE`，可额外授予：`GRANT EXECUTE ON DBMS_FLASHBACK TO your_sync_user;`

---

## ⚙️手动编译
> 先确保环境已安装JDK和Maven
```bash
$ git clone https://gitee.com/ghi/dbcbc.git
$ cd dbcbc
$ chmod u+x build.sh
$ ./build.sh
```
## 🏆[性能测试](https://gitee.com/ghi/dbcbc/wikis/%E5%BF%AB%E9%80%9F%E4%BA%86%E8%A7%A3/%E6%80%A7%E8%83%BD%E6%B5%8B%E8%AF%95)
#### 全量同步

| 系统 | 机器配置 |  数据量 |  耗时 |
|---|---|---|---|
| Mac | Apple M3 Pro 12核心 内存18GB | 1亿条 | 31分50秒 |
| Linux | Intel(R) Xeon(R) CPU E5-2696 v3B 8核心 内存48GB | 1亿条 | 37分52秒 |
| Windows | AMD Ryzen 7 5800x 8核心 12GB | 1亿条 | 57分43秒 |

#### 增量同步
| 系统 | 机器配置 |  分配内存 |  TPS | 峰值 |
|---|---|---|---|---|
| Mac | Apple M3 Pro 12核心 内存18GB | 4GB | 8112/秒 | 11000/秒 |
| Linux | Intel(R) Xeon(R) CPU E5-2696 v3B 8核心 内存48GB | 4GB | 8000/秒 | 10000/秒 |
| Windows | AMD Ryzen 7 5800x 8核心 12GB | 4GB | 7553/秒 | 9000/秒 |

### 连接与同步 FAQ

* **MySQL 无法连接**：默认驱动版本为 8.0.21，MySQL 5.x 可能需要手动替换驱动 [mysql-connector-java-5.1.40.jar](https://gitee.com/ghi/dbcbc/attach_files) 到 `lib` 目录。
* **SQL Server 无法连接**：案例：[TLS10 与 TLS12 不兼容](https://gitee.com/ghi/dbcbc/issues/I4PL46?from=project-issue)
* **同步数据乱码**：案例：[mysql8 导入 sqlserver2008R2 后 nvarchar 乱码](https://gitee.com/ghi/dbcbc/issues/I4JXY0)
* **如何开启远程 debug 模式？**：[说明](https://gitee.com/ghi/dbcbc/issues/I63F6R)
