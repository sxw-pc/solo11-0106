# 馆藏盘点采录台

社区图书馆离线 RFID 盘点。冻结馆藏、借阅和读写器布局快照，导入本地扫描及借还日志，重建每册应在馆状态并输出差异和证据。仅用合成数据，不联网。

Python 3.11+，`python -m pip install -e .` 安装。`python -m shelfscan demo` 一条命令生成 SQLite 与 JSON/Markdown 报告，`python -m shelfscan --help` 查看独立命令，`python -m pytest -q` 运行测试。

Batch 指向 Copy，Reader 给出读写器架位和有效时间窗；Source 以原字节哈希去重，Event 以设备与序号幂等，重复扫描保留信号。Decision 结合借还时序、位置、信号强度给出在架、串架、缺失、借出或待核。多架信号接近时待核，不任意归位；借出册不判丢失。Issue 保留解析、身份、时序和定位错误，报告包含快照版本、来源哈希、事件 ID 和候选架位分数。批次状态为待接收、已校验、待核、已判定、作废，问题码由 issue.code 表示。

Run 记录重算生命周期；Evidence 将每条事件与来源、载荷哈希相连。导入和判定分别用事务保证原子性。

## 来源文件内容寻址归档

`archive` 子命令把 source.raw 原字节在线迁入不可改写块仓（`<db>.chunks/`，块按内容 SHA-256 命名、原子写入、同内容物理只存一份），每来源一份有序 manifest 记录块序、字节数、表示与版本。两种无损表示：`fixed_64` 每 64 字节切块；`record_chunks` 每条原始 JSONL 行连同原分隔字节为一块，前导 BOM 与无末尾换行的末尾状态独立成块。按 manifest 读取必拼回原文件逐字节内容并重得 source.sha256，不重序列化 JSON。

迁移期旧 raw BLOB 与新 manifest 并存；只有重构字节与旧 BLOB/SHA 完全一致才发布 manifest、用 source_state.generation 比较交换切换读指针并清理冗余 BLOB。读者打开时钉住所见世代（reader_pin），GC 只回收不被当前/旧已发布 manifest、building manifest 或活跃读者引用的块，不按固定宽限秒数猜测；钉住 generation 0 的读者会推迟 BLOB 清理至其关闭。崩溃可发生在块写入、发布、切换、清理任意两点之间，重启后至少有一份完整可核验原件，`migrate` 幂等续迁。

```
python -m shelfscan archive --db X migrate --rep record_chunks [--source-id N]
python -m shelfscan archive --db X verify [--source-id N]
python -m shelfscan archive --db X cat --source-id N --out FILE
python -m shelfscan archive --db X stats      # 数据库文件/块仓实际占用与逻辑量分列
python -m shelfscan archive --db X gc [--vacuum]
python -m shelfscan archive --db X register --case-id C --fixture F
python -m shelfscan archive --db X pins
```

`stats` 的 physical 是数据库文件加块仓文件的实际字节，logical 是各来源原字节合计；去重只有在 BLOB 清理并 GC/VACUUM 后才体现为磁盘节省，逻辑去重数不冒充磁盘节省。
