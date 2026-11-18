# 馆藏盘点采录台

社区图书馆离线 RFID 盘点。冻结馆藏、借阅和读写器布局快照，导入本地扫描及借还日志，重建每册应在馆状态并输出差异和证据。仅用合成数据，不联网。

Python 3.11+，`python -m pip install -e .` 安装。`python -m shelfscan demo` 一条命令生成 SQLite 与 JSON/Markdown 报告，`python -m shelfscan --help` 查看独立命令，`python -m pytest -q` 运行测试。

Batch 指向 Copy，Reader 给出读写器架位和有效时间窗；Source 以原字节哈希去重，Event 以设备与序号幂等，重复扫描保留信号。Decision 结合借还时序、位置、信号强度给出在架、串架、缺失、借出或待核。多架信号接近时待核，不任意归位；借出册不判丢失。Issue 保留解析、身份、时序和定位错误，报告包含快照版本、来源哈希、事件 ID 和候选架位分数。批次状态为待接收、已校验、待核、已判定、作废，问题码由 issue.code 表示。

Run 记录重算生命周期；Evidence 将每条事件与来源、载荷哈希相连。导入和判定分别用事务保证原子性。

## 实体读写器固件批量升级

`firmware_lab` 只提供本地设备端点（`state`/`invoke`，带修订号与操作 ID）、持久设备状态和故障夹具；`firmware_release` 在其上实现可恢复的批量升级，成功与否只以设备端点实况（版本、修订、操作记录）为准，不做离线成功标记。

- `fw-seed`：用 `firmware-config.json`/`firmware-events.jsonl`/`firmware-case.json` 初始化馆藏、导入真实扫描并登记读写器；`register` 仅接受已存在且有扫描证据的读写器。
- `fw-plan`：升级前冻结目标读写器、最近真实扫描来源与事件哈希、单元归属、当前版本/修订号、目标包哈希、操作 ID 与合同，写入可重放意图（`firmware_intent*`）和追加式日志（`firmware_journal`）。同一意图 ID 重放必须逐字节一致，操作 ID 不得换设备或换动作复用。
- `fw-run`：以设备当前修订号做比较交换。安装返回超时后先查同一设备状态与操作记录——R2 已实际提交到 v2 时只确认不重装，也不按超时记失败；R3 拒绝后按合同回退。可重复调用，中断后从日志与设备实况继续。
- 两种合法合同：`atomic_fleet` 在任一读写器失败时把本次已升级设备全部安全回退，最终三台均为 v1；`cell_isolated` 仅回退失败单元内由本次改变的设备，CELL-1 的 R1/R2 保持 v2，CELL-2 的 R3 留在 v1。
- 外部维护改动设备修订时停止覆盖并置 `awaiting_manual`，绝不为满足目标强制写版本。仅固件版本与单元发布合同均确认的读写器才在 `firmware_reader_availability` 标记采录可用。
- `fw-report`：输出逐设备版本/修订/操作与确认状态、单元发布状态、扫描证据链接和可核验报告（JSON + Markdown + `.sha256` 边车）。原 event/source 证据与 source SHA 保持不变，不重算 RFID 册状态、路线或馆藏赔偿。
- `fw-demo`：在同一故障夹具下并行演示两种合同，产生不同的实体固件版本与可用单元。

四个可恢复中断点（测试以 `crash_after` 屏障模拟）：写意图后、设备提交后确认前、跨单元发布中、回退提交后确认前。
