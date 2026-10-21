# Issue #6669 设计:Spark MERGE INTO 主键表目标侧列裁剪

- Issue: https://github.com/apache/paimon/issues/6669
- 状态:已实现(2026-10-18),spark3/spark4 全矩阵回归全绿,未提交
- 影响模块:`paimon-spark/paimon-spark-common` 与 `paimon-spark/paimon-spark-4.0`——同一 `MergeIntoPaimonTable.scala` 的两份逐字节相同拷贝(已 diff 确认),需同步修改

## 1. 背景与目标

issue 场景:1M 行源表 MERGE INTO 10M 行主键表:

```sql
MERGE INTO target USING source ON target.id = source.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

实测物理计划中 Paimon 对目标表全列扫描 + 全列 shuffle;Iceberg 同查询只读 join key 列。

目标:主键表 MERGE INTO 时,join 阶段目标侧(及源侧)只扫描、shuffle 真正被引用的列。对上述 SQL,目标侧从全列降为 `id` 一列。**只做列裁剪,不改正确性行为。**

收益边界:UPDATE 未提及的列经对齐退化为目标列自赋值(`PaimonAssignmentUtils.applyAssignments` L171),故显式部分列 `UPDATE SET`、`NOT MATCHED BY SOURCE UPDATE SET *`、merge-schema 的 PreserveTarget 均引用全部目标列,裁剪自动退化为全列——机制自然处理、无正确性问题,但收益集中在 `UPDATE SET *` 等赋值不引用目标列的形态(issue 场景)。issue 回复口径据此,不过度承诺。

## 2. 根因(代码事实)

MERGE INTO 两条路径中,裁剪能力不同:

| 路径 | 触发条件 | 现状 |
|---|---|---|
| V2 row-level ops | Spark ≥3.5 且 append-only 表 | scan 按 Spark 下推的 `requiredSchema` 构建,已有裁剪,不动 |
| V1 自定义命令 `MergeIntoPaimonTable` | 所有主键表(及旧 Spark 全部表) | **全列,本设计改造对象** |

根因链(均在 `MergeIntoPaimonTable.scala`):

1. 主键表入口 `performMergeForPkTable`(L100)把完整目标 Dataset 送进 `constructChangedRows`;
2. join 为 FullOuter,两侧投影均带完整 output;
3. 合并语义全部在 `joinedDS.mapPartitions(processor.processPartition)`(L338)内,lambda 用 encoder 将整行 join 结果物化为 Row——Spark 优化器无法穿透该黑盒做 ColumnPruning;
4. `noopOutput`/`keepOutput`(L278)引用全部 target 列,但这些行随后被 `filterNot(unusedRow)` 丢弃——"计划上活着、运行时已死"的引用,阻止了一切下推。

佐证能力存在、只是没用上:

- `PaimonBaseScanBuilder` 实现 `SupportsPushDownRequiredColumns`(`pruneColumns`),扫描层支持列裁剪;
- 同命令 `checkMatchRationality`(L341)的 join 是纯计划级,Spark 已自动裁剪到 key 列;
- 非主键表路径本身就是两阶段(`findTouchedFiles` key join → 只重写 touched files),无此问题。

## 3. 方案

### 3.1 裁剪范围与门控

只对 `performMergeForPkTable` 调用启用:`constructChangedRows` 新增参数 `pruneJoinColumns: Boolean = false`,主键表调用点传 `true`,其余两个调用点(DV、非 PK 重写路径)不传、行为不变。

选择显式参数而非按 flags 推断:推断与调用点语义隐式耦合,显式声明更可审。

### 3.2 机制

改动全部在 `constructChangedRows` 内:

1. **计算所需列集合**。收集表达式全集 `E` = `mergeCondition` ∪ 三类 action 的 condition ∪ 三类 action 的每个 `Assignment.value`(assignment 的 key 只是列标识,不参与求值,不收集)。则:
   - `neededTarget` = `E.flatMap(_.references)` 中 exprId ∈ 目标侧 output 的属性;
   - `neededSource` = 同理,exprId ∈ `sourceTable.output`。
2. **Delete 门控**:若 `remainDeletedRow && (matchedActions ++ notMatchedBySourceActions).exists(isInstanceOf[DeleteAction])`,则 `neededTarget` = 全列(此时 -D 行需要完整目标行,见 3.4)。
3. **join 前显式 Project**:`Project(neededTarget :+ TARGET_ROW_COL, targetPlan)`、`Project(neededSource :+ SOURCE_ROW_COL, sourceTable)` 替代现有全列投影。扫描层由 `pruneColumns` 下推,shuffle 只携带所需列。
4. **模板 null 填充**:`targetOutput`(noop/keep/delete 输出行模板)改为按原列序构建——保留列用原属性,被裁列用 `Literal(null, attr.dataType)`。输出 schema(`targetTable.schema.fields` 构建)不变,encoder 不变。

安全性论证——**null 槽位永远不会到达 writer**:输出行中,INSERT/UPDATE_AFTER 的值全部来自 aligned assignments(表达式求值,非模板);NOOP 行被 `filterNot` 丢弃;DELETE 行只在 Delete 门控放行时存在(此时未裁剪);keep 行只在非 PK 路径激活(`fromTouchedFile` 依赖 `FILE_TOUCHED_COL`,主键路径 joined schema 无此列,恒 false)。故 null 填充只出现在必被丢弃或未裁剪的行上。

### 3.3 不变的边界

- 非主键表两阶段路径:keep 行需整行写回,不裁剪;
- DV 路径:`-D` 行模板同样引用目标列,虽有进一步裁剪空间(-D 行 payload 实际只用 file_path/row_index),按外科手术原则本次不碰;
- row tracking、`extraMetadataCols`:所在调用点均未启用裁剪参数,不受影响;
- `MergeIntoPaimonDataEvolutionTable`(data evolution 独立命令):不在范围。

### 3.4 放弃的备选

- **Iceberg 式两阶段 group-based rewrite**(先 key join 定位受影响文件/行,再全列重读):主键表路径输出的是 changelog(变更行),不重写整文件,两阶段不会带来额外收益,反而引入第二轮读取。单阶段 + 列裁剪即可达到同等 I/O 剖面。
- **-D 行裁到 key 列**:LSM merge engine 对 -D 行 value 的消费语义随引擎与配置变化,已核实:`PartialUpdateMergeFunction.java:209/380` 会按字段消费 DELETE 行(partial-update 应用 -D 行非 null 字段,sequence group 下还参与序列比较,见 L149 注释),`compact/aggregate/AggregateMergeFunction.java:84` 按 `ignore-delete`/`remove-record-on-delete` 分支。null 掉 payload 会实际改变合并结果,属 core 写路径语义,超出本次单文件优化半径。不做,直接以 Delete 门控退化为全列。
- **`checkMatchRationality` 的额外 join 消除/合并**:独立热点(多一轮 key-only shuffle),与正确性门控耦合,不并入本次改动。可作为 issue 回复中提出的后续项。

## 4. 验证计划(成功标准)

1. **helper 单测**(新增,`MergeIntoPaimonTable` object 内 `private[commands]` 纯函数):属性收集覆盖 target/source 两侧、assignment value 引用、Delete 门控、无条件引用时的空集。
2. **新增正确性 case**(加入 `paimon-spark-ut` 的 `MergeIntoTableTestBase` 体系,全版本继承执行):
   - 宽表(≥6 列)主键表 `UPDATE SET *` + `INSERT *`,merge 后全表内容与手算 expected 逐行一致——被裁列的值不被破坏即证明 null 填充未泄漏;
   - assignment 引用目标列(`SET c = target.c + source.x`)、merge 条件引用目标非 key 列、`WHEN MATCHED THEN DELETE`(门控退化全列)各一 case。
3. **既有套件回归**:`MergeIntoTableTest` / `MergeIntoNotMatchedBySourceTest` / `MergeIntoAlignmentTest` 在 **3.2/3.3/3.4/3.5/4.0/4.1** 全绿。3.2 起每个版本模块都有 `MergeIntoTableTest`,且 Spark <3.5 恒走 V1 命令(shim 硬编码不支持 V2 row-level ops),是受影响版本,必须纳入。新增 case 写在共享基座 `paimon-spark-ut` 的 `MergeIntoTableTestBase` 一处——该模块按 profile 的 `scala.binary.version` 编译(spark3→2.12,spark4→2.13),六个版本模块全部继承执行。示例命令(`-Pspark3` 一个 profile 即带出 3.2–3.5 四个模块,`-Pspark4` 需 JDK 17):
   ```shell
   SUITES="org.apache.paimon.spark.commands.MergeIntoPaimonTablePruneColumnsTest,\
   org.apache.paimon.spark.sql.MergeIntoAlignmentTest,\
   org.apache.paimon.spark.sql.MergeIntoPrimaryKeyBucketedTableTest,\
   org.apache.paimon.spark.sql.MergeIntoPrimaryKeyNonBucketTableTest,\
   org.apache.paimon.spark.sql.MergeIntoAppendBucketedTableTest,\
   org.apache.paimon.spark.sql.MergeIntoAppendNonBucketedTableTest,\
   org.apache.paimon.spark.sql.V2MergeIntoPrimaryKeyBucketedTableTest,\
   org.apache.paimon.spark.sql.V2MergeIntoPrimaryKeyNonBucketTableTest,\
   org.apache.paimon.spark.sql.V2MergeIntoAppendBucketedTableTest,\
   org.apache.paimon.spark.sql.V2MergeIntoAppendNonBucketedTableTest"
   # spark3(3.2–3.5 一次跑完,约 16 min)
   mvn -pl paimon-spark/paimon-spark-3.2,paimon-spark/paimon-spark-3.3,paimon-spark/paimon-spark-3.4,paimon-spark/paimon-spark-3.5 \
     -am -Pfast-build -Pspark3 -DwildcardSuites="$SUITES" -Dtest=none -DfailIfNoTests=false test
   # spark4(先 clean 共享模块;JDK 17,约 17 min)
   mvn -pl paimon-spark/paimon-spark-common,paimon-spark/paimon-spark-ut clean -q
   mvn -pl paimon-spark/paimon-spark-4.0,paimon-spark/paimon-spark-4.1 \
     -am -Pfast-build -Pspark4 -DwildcardSuites="$SUITES" -Dtest=none -DfailIfNoTests=false test
   ```
   实测注意(2026-10-18):本仓 scalatest-maven-plugin 2.1.0 的 `wildcardSuites` **不支持 `*` 通配**,须用逗号分隔的精确类名,未匹配的类名被静默跳过(故各模块可共用上面的并集列表);spark3↔spark4 切换 profile 前必须先 clean `paimon-spark-common` 与 `paimon-spark-ut`,否则 2.12 字节码混入 2.13 运行时报 `NoSuchMethodError: wrapRefArray`。实测结果:ut 31 用例(2.12/2.13 各一轮,含 helper 单测与 MergeIntoAlignmentTest)全绿,3.2=240、3.3=240、3.4=268、3.5=536(含 V2)、4.0=536、4.1=536,零失败。
   实测已按上述命令跑通 spark3 四模块与 spark4 两模块的全量 MergeInto 套件(见下方结果)。
4. **裁剪生效演示**(人工验收,非自动化):注意 `EXPLAIN` 看不到内部计划——`MergeIntoPaimonTable` 是 Command,内部 Dataset 在 `run()` 时才构建,`EXPLAIN` 只显示 `Execute MergeIntoPaimonTable` 节点。正确观察方式:Spark UI SQL tab(每条内部查询单独展示)或临时 `QueryExecutionListener` 抓取内部查询的 FileScan `readSchema`,确认目标表读列仅为所需列。截图用于 issue 回复——对外发布前须经用户审核。**该验证仅证明本次生效,不构成防未来退化的自动化防线**;裁剪退化只损性能不损正确性,回归保护依赖第 1、2 条的正确性 case。

## 5. 风险与回滚

- 最大风险:门控失效导致 null 模板行被写出。防线 = 3.2 的不变量论证 + 4.2 宽表 case 显式覆盖非 PK/DV/row-tracking 全分支。
- 改动集中在上述两份同步拷贝内,加一个参数与若干投影构建,回滚 = revert 单 commit(同时覆盖两文件)。
- 跨版本:机制仅用 `Project`/`Literal`/`references` 等跨版本稳定 API;`Assignment.value` 取值已由 `PaimonUpdateAction` 解包器处理版本差异;仍按矩阵回归。

## 6. 明确不做与已知限制

- 含 `WHEN MATCHED` / `WHEN NOT MATCHED BY SOURCE` `THEN DELETE` 的 merge 暂不裁剪(已知限制,在 issue 回复中明确说明;-D key-only 列为后续项);
- 非主键表路径、DV 路径的进一步裁剪;
- `checkMatchRationality` 额外 join 的消除;
- 广播/AQE 等运行时优化(裁剪后 shuffle 变小,AQE 自主决策,无需代码介入);
- Spark 4.1 V2 路径(已具备裁剪);
- 裁剪生效的自动化计划断言(EXPLAIN 跨版本不稳定,且 Command 内部计划需额外钩子才能暴露;性能退化由 review 把关,不建防线)。
