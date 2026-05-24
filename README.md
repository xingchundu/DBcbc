

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

### DDL 结构迁移

| 功能 | 说明 |
|------|------|
| **两阶段迁移** | ① **表结构迁移**（建表/补字段/索引）→ ② **数据库对象迁移**（过程/函数/视图等）；阶段一完成后可继续阶段二，也可仅做表结构 |
| **SQL Server → PostgreSQL** | 新增表结构迁移链路（类型映射 `sqlserver2pg`）及对象迁移（视图、序列、存储过程、函数、触发器等 T-SQL→PL/pgSQL 最大努力转换） |
| **Oracle 对象迁移** | 阶段二支持 Oracle → PostgreSQL / 达梦 / MySQL，迁移存储过程、函数、触发器、包、视图、同义词、序列等 |
| **迁移路径提示** | 选择源/目标连接后，页面显示当前路径及是否支持对象迁移 |
| **迁移日志着色** | ERROR 红色，WARN 蓝色，SUCCESS 绿色 |
| **表结构完成统计** | 阶段一结束后展示：源库对象总数（表、索引）、目标库对象总数（表、索引） |
| **对象迁移统计** | 阶段二结束后展示：成功 / 警告 / 失败 / 跳过 / 合计 |
| **人工核查提示** | 跨库过程/函数/触发器转换后默认 WARN；Job、权限/角色类对象不自动执行，仅写入日志需人工处理 |

### 界面调整

| 功能 | 说明 |
|------|------|
| **插件管理菜单隐藏** | 侧栏「插件管理」已隐藏，功能仍可通过 `/plugin` 直接访问 |

### 达梦（DM）增量同步 → PostgreSQL

| 功能 | 说明 |
|------|------|
| **日志增量** | 支持达梦作为源、PostgreSQL 作为目标的 **日志增量** 同步（基于 DM `DBMS_LOGMNR` 归档挖掘） |
| **前置条件** | 达梦需开启归档（`ARCH_INI=1`）、逻辑日志（`RLOG_APPEND_LOGIC=1/2/3/4`），同步账号需 DBA 或 LogMiner 相关权限 |
| **配置方式** | 连接管理配置 DM 源 + PG 目标 → 驱动管理选择 **增量**、策略 **日志** → 配置表映射后启动 |

> 说明：定时增量（timing）策略对达梦原本可用；本次主要补齐 **日志增量**（log）。跨库 DDL 自动同步仍不支持，仅同步 DML 变更。

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

- **阶段一**：表结构迁移（建表/补字段/索引，不迁移业务数据）
- **阶段二**：数据库对象迁移（存储过程、函数、触发器、视图、序列等）

**支持路径**：Oracle → PostgreSQL / 达梦 / MySQL、**SQL Server → PostgreSQL**。

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

#### 3. DDL 与类型映射（Oracle / SQL Server → PostgreSQL 等）

- 结构迁移模块内对类型映射做了增强，例如 Oracle 中带精度的 `TIMESTAMP(n)`、SQL Server 类型等与未映射类型会按约定落到 PostgreSQL 可执行类型（如 `TEXT`），避免生成目标端不存在的类型名导致建表失败。若遇特殊类型仍失败，请以页面日志为准并检查两端方言差异。
- 阶段二的过程/函数/触发器为 **最大努力语法替换**，建议人工核查后再上线；Job、权限/角色仅输出说明，需手动在目标库执行。

#### 4. 操作建议小结

| 场景 | 建议 |
|------|------|
| DDL 在目标实例上 **新建了 database** | DDL 页刷新连接库列表；必要时重选连接器缓存的库列表 |
| 映射里目标库与 `psql` 里不一致 | 检查连接器 JDBC URL 库名是否与 `targetDatabase` 一致（PostgreSQL 尤其重要） |
| 目标表太少、但库里明明很多表 | 先「取消过滤」排除映射过滤；再「刷新表」；仍不对则检查 JDBC URL |
| 迁移报错 | 查看 DDL 迁移页日志中的红色错误信息 |

> **补充**：DDL 相关接口：`POST /ddl/transfer`（阶段一：表结构迁移）；`POST /ddl/transferObjects`（阶段二：对象迁移，参数 `objectTypes` 如 `PROCEDURE,FUNCTION,VIEW`）；`POST /ddl/refreshConnectorMeta`（刷新连接器缓存的库列表，与 DDL 页 **「刷新连接库列表」** 按钮一致）。

---

## 安装部署

### 方式一 下载安装包
1. 安装[JDK 1.8](https://www.oracle.com/java/technologies/jdk8-downloads.html)（省略详细）
2. 下载安装包[dbcbc-x.x.x.zip](https://gitee.com/ghi/dbcbc/releases)（也可手动编译）
3. 解压安装包，Window执行bin/startup.bat，Linux执行bin/startup.sh
4. 打开浏览器访问：http://127.0.0.1:18686
5. 账号和密码：admin/admin

### 方式二 🐳 docker
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

* MySQL无法连接。默认使用的驱动版本为8.0.21，如果为mysql5.x需要手动替换驱动 [mysql-connector-java-5.1.40.jar](https://gitee.com/ghi/dbcbc/attach_files) 
* SQLServer无法连接。案例：[驱动程序无法通过使用安全套接字层(SSL)加密与 SQL Server 建立安全连接。错误:“The server selected protocol version TLS10 is not accepted by client preferences [TLS12]”](https://gitee.com/ghi/dbcbc/issues/I4PL46?from=project-issue) 
* 同步数据乱码。案例：[mysql8表导入sqlserver2008R2后，sqlserver表nvarchar字段内容为乱码](https://gitee.com/ghi/dbcbc/issues/I4JXY0) 
* [如何开启远程debug模式？](https://gitee.com/ghi/dbcbc/issues/I63F6R)  
