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

## 已知限制

- 上游做 DDL 之后，管道仍按首次解析到的列定义投影，新增列会被丢弃，删除的列在目标表也会被一并清掉。
- 位点在写目标表之前提交，进程在两者之间中断会丢行。
- 同一主键的事件按到达顺序覆盖，不比较 version。
