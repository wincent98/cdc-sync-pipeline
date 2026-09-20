# cdc-sync-pipeline

把上游 MySQL binlog 的行变更增量同步到目标表的管道。

## 模块

| 包 | 职责 |
|----|------|
| `event` | `ChangeEvent` 是解码后的一行变更，带 offset、主键、操作类型和 version；`DeadLetterQueue` 收容管道拒绝应用的事件 |
| `schema` | `SchemaRegistry` 保存每张表当前的列定义，上游 DDL 会注册新的 `TableSchema` |
| `offset` | `OffsetStore` 保存消费位点，重启后从 `committed() + 1` 继续 |
| `sink` | `RowStore` 是目标表的内存替身 |
| `pipeline` | `SyncPipeline.consume` 是消费主链路 |

## 构建与运行

```bash
mvn -o package
java -jar target/cdc-sync-pipeline-2.1.0.jar
```

回放一段固定的 binlog 流，其中包含 5 次 DDL、一条乱序事件和一次模拟崩溃，跑完打印一致性报告。
退出码 0 表示目标表与期望终态一致，1 表示存在偏差。

## 测试

```bash
mvn -o test
```

## 语义保证

- Schema 演进：新增列自动落到目标表；删除列保留最后一次历史值；INT→BIGINT、VARCHAR 变长等放宽变更静默兼容；类型收窄（BIGINT→INT、VARCHAR 变短）的携带事件进入死信队列，管道继续消费后续事件。
- 断点续跑：行落库成功后才提交位点，同一主键的写库与位点提交在同一临界区内原子完成；重启后从 `committed() + 1` 重放，已落库的行不会重复写入。
- 乱序合并：同一主键按 version 单调合并，低于目标表当前版本的事件（含过期 DELETE）直接丢弃并推进位点。
- SchemaRegistry 的缓存失效与位点提交处于同一临界区，重启后不会用旧列映射解析新事件；并发粒度为主键级（分段锁），不串行化整个 consume。
