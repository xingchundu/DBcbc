

> 特点
* 组合驱动，自定义库同步到库组合，关系型数据库与非关系型之间组合，任意搭配表同步映射关系
* 实时监控，驱动全量或增量实时同步运行状态、结果、同步日志和系统日志
* 开发插件，自定义转化同步逻辑

> 项目地址

## 🌈应用场景
| 连接器        | 数据源 | 目标源 | 支持版本(包含以下)    |
|------------|---|---|---------------|
| MySQL      | ✔ |  ✔ | 5.7.19以上      |
| Oracle     | ✔ |  ✔ | 10g-19c       |
| SqlServer  | ✔ |  ✔ | 2008以上        |
| PostgreSQL | ✔ |  ✔ | 9.5.25以上      |
| Sqlite     | ✔ |  ✔ | 2以上           |
| ES         | ✔ |  ✔ | 6.0.0-8.15.3  |
| Kafka      | ✔ |  ✔ | 2.10-0.9.0.0以上 |
| File       | ✔ |  ✔ | *.txt, *.unl  |
| Http       | ✔ |  ✔ |   |
| SQL        | ✔ |  | 支持以上关系型数据库    |
| 后期计划       | Redis | |               |

## DDL 迁移（库表结构迁移）

在 Web 侧提供 **「DDL迁移」** 入口（侧栏），用于在已配置的 **关系型数据库连接** 之间做 DDL/结构迁移（以连接管理中的连接器为源与目标）。迁移过程可在页面查看执行日志；失败信息会以醒目标示展示。

### 使用说明（简要）

1. 在 **连接管理** 中配置好源库、目标库连接器。
2. 打开 **DDL迁移**，选择源/目标连接及库、schema 等选项，执行迁移。
3. 迁移完成后，若目标侧 **新建了数据库**，连接器里缓存的库名列表可能仍是旧的，请在 DDL 页面使用 **「刷新连接库列表」**（或等价操作），再在驱动/映射里选择新库。

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

#### 3. DDL 与类型映射（Oracle → PostgreSQL 等）

- 结构迁移模块内对类型映射做了增强，例如 Oracle 中带精度的 `TIMESTAMP(n)` 等与未映射类型会按约定落到 PostgreSQL 可执行类型（如 `TEXT`），避免生成目标端不存在的类型名导致建表失败。若遇特殊类型仍失败，请以页面日志为准并检查两端方言差异。

#### 4. 操作建议小结

| 场景 | 建议 |
|------|------|
| DDL 在目标实例上 **新建了 database** | DDL 页刷新连接库列表；必要时重选连接器缓存的库列表 |
| 映射里目标库与 `psql` 里不一致 | 检查连接器 JDBC URL 库名是否与 `targetDatabase` 一致（PostgreSQL 尤其重要） |
| 目标表太少、但库里明明很多表 | 先「取消过滤」排除映射过滤；再「刷新表」；仍不对则检查 JDBC URL |
| 迁移报错 | 查看 DDL 迁移页日志中的红色错误信息 |

> **补充**：DDL 相关接口：`POST /ddl/transfer`（执行迁移）；`POST /ddl/refreshConnectorMeta`（刷新连接器缓存的库列表，与 DDL 页 **「刷新连接库列表」** 按钮一致）。

---

## 安装部署

### 方式一 下载安装包
1. 安装[JDK 1.8](https://www.oracle.com/java/technologies/jdk8-downloads.html)（省略详细）
2. 下载安装包[dbsyncer-x.x.x.zip](https://gitee.com/ghi/dbsyncer/releases)（也可手动编译）
3. 解压安装包，Window执行bin/startup.bat，Linux执行bin/startup.sh
4. 打开浏览器访问：http://127.0.0.1:18686
5. 账号和密码：admin/admin

### 方式二 🐳 docker
* 阿里云镜像
```shell
# 社区版
docker pull scxhtb-registry.cn-hangzhou.cr.aliyuncs.com/xhtb/dbsyncer:latest
docker pull scxhtb-registry.cn-hangzhou.cr.aliyuncs.com/xhtb/dbsyncer:2.0.8
# 专业版
docker pull scxhtb-registry.cn-hangzhou.cr.aliyuncs.com/xhtb/dbsyncer-enterprise:latest
docker pull scxhtb-registry.cn-hangzhou.cr.aliyuncs.com/xhtb/dbsyncer-enterprise:2.0.8
```
* 运行命令
```shell
docker run -d \
  --name=dbsyncer \
  --restart=unless-stopped \
  -p 18686:18686 \
  -e TZ="Asia/Shanghai" \
  -m 5g \
  --memory-swap=5g \
  -v /opt/dbsyncer/data:/app/dbsyncer/data \
  -v /opt/dbsyncer/logs:/app/dbsyncer/logs \
  -v /opt/dbsyncer/plugins:/app/dbsyncer/plugins \
  --log-driver json-file \
  --log-opt max-size=100m \
  --log-opt max-file=7 \
  scxhtb-registry.cn-hangzhou.cr.aliyuncs.com/xhtb/dbsyncer:latest

# 本地日志
ls -la /opt/dbsyncer/logs

# 容器日志
docker logs --tail 20 dbsyncer
# 容器实时日志（Ctrl+C退出）
docker logs -f dbsyncer
# 进入容器内部
docker exec -it dbsyncer /bin/bash
# 查看容器日志
ls -la /app/dbsyncer/logs

# 停止容器
docker stop dbsyncer
# 启动容器
docker start dbsyncer
# 重启容器
docker restart dbsyncer
# 删除容器（需先停止）
docker rm dbsyncer
```

## ⚙️手动编译
> 先确保环境已安装JDK和Maven
```bash
$ git clone https://gitee.com/ghi/dbsyncer.git
$ cd dbsyncer
$ chmod u+x build.sh
$ ./build.sh
```
## 🏆[性能测试](https://gitee.com/ghi/dbsyncer/wikis/%E5%BF%AB%E9%80%9F%E4%BA%86%E8%A7%A3/%E6%80%A7%E8%83%BD%E6%B5%8B%E8%AF%95)
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

* MySQL无法连接。默认使用的驱动版本为8.0.21，如果为mysql5.x需要手动替换驱动 [mysql-connector-java-5.1.40.jar](https://gitee.com/ghi/dbsyncer/attach_files) 
* SQLServer无法连接。案例：[驱动程序无法通过使用安全套接字层(SSL)加密与 SQL Server 建立安全连接。错误:“The server selected protocol version TLS10 is not accepted by client preferences [TLS12]”](https://gitee.com/ghi/dbsyncer/issues/I4PL46?from=project-issue) 
* 同步数据乱码。案例：[mysql8表导入sqlserver2008R2后，sqlserver表nvarchar字段内容为乱码](https://gitee.com/ghi/dbsyncer/issues/I4JXY0) 
* [如何开启远程debug模式？](https://gitee.com/ghi/dbsyncer/issues/I63F6R)  
