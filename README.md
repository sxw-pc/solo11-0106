# 馆藏盘点采录台

社区图书馆离线 RFID 盘点。冻结馆藏、借阅和读写器布局快照，导入本地扫描及借还日志，重建每册应在馆状态并输出差异和证据。仅用合成数据，不联网。

Python 3.11+，`python -m pip install -e .` 安装。`python -m shelfscan demo` 一条命令生成 SQLite 与 JSON/Markdown 报告，`python -m shelfscan --help` 查看独立命令，`python -m pytest -q` 运行测试。

Batch 指向 Copy，Reader 给出读写器架位和有效时间窗；Source 以原字节哈希去重，Event 以设备与序号幂等，重复扫描保留信号。Decision 结合借还时序、位置、信号强度给出在架、串架、缺失、借出或待核。多架信号接近时待核，不任意归位；借出册不判丢失。Issue 保留解析、身份、时序和定位错误，报告包含快照版本、来源哈希、事件 ID 和候选架位分数。批次状态为待接收、已校验、待核、已判定、作废，问题码由 issue.code 表示。

Run 记录重算生命周期；Evidence 将每条事件与来源、载荷哈希相连。导入和判定分别用事务保证原子性。

信号融合契约（fusion_contract）随快照冻结：`strategy`（cumulative / burst_neutral）、`burst_window_seconds` 突发窗、`confidence_multiplier` 第一二名置信倍数与契约版本。原始扫描与心跳全部保留，判定只读取不改写事件，两种策略可随时基于同一批事件重算：累计策略对同册候选架位信号直接求和（保留原语义）；突发中和按同一设备、同一册、同一架位、以首读为锚的时间窗聚成 read burst，每个 burst 仅贡献峰值（窗边界含首尾，超窗另起 burst，不同设备永不归并），再按冻结的置信倍数比较候选架位。借还时序与借出状态独立于融合策略，切换策略不改变其他册状态。Decision 明细含原始读数、burst 边界、两种分数及最终采用的快照策略。
