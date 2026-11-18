# 馆藏盘点采录台

社区图书馆离线 RFID 盘点。冻结馆藏、借阅和读写器布局快照，导入本地扫描及借还日志，重建每册应在馆状态并输出差异和证据。仅用合成数据，不联网。

Python 3.11+，`python -m pip install -e .` 安装。`python -m shelfscan demo` 一条命令生成 SQLite 与 JSON/Markdown 报告，`python -m shelfscan --help` 查看独立命令，`python -m pytest -q` 运行测试。

Batch 指向 Copy，Reader 给出读写器架位和有效时间窗；Source 以原字节哈希去重，Event 以设备与序号幂等，重复扫描保留信号。Decision 结合借还时序、位置、信号强度给出在架、串架、缺失、借出或待核。多架信号接近时待核，不任意归位；借出册不判丢失。Issue 保留解析、身份、时序和定位错误，报告包含快照版本、来源哈希、事件 ID 和候选架位分数。批次状态为待接收、已校验、待核、已判定、作废，问题码由 issue.code 表示。

Run 记录重算生命周期；Evidence 将每条事件与来源、载荷哈希相连。导入和判定分别用事务保证原子性。

## 在架馆藏温控（climate_control）

`climate_register` 先把温控案例与 `ingest` 冻结的真实架位扫描核对：每架必须有同架读写器的 scan 事件，传感器 CSV 原字节与 SHA256、初始温度、每时段增温、保护上限、共享冷却机能力、分时电价和两种运营目标一并冻结进 `climate_case`。传感器身份冲突、时区缺失、缺增温、非物理冷却量或电价非法时直接拒绝注册，不产生任何局部命令。

求解器是精确动态规划：状态为（各架结束温度、各架累计热暴露），逐时段枚举冷却机的全部可行分配（最多 `max_shelves_per_period` 架，设备不可重复占用），每个状态只保留费用最低的前驱。终点按合同总暴露上限过滤后，依授权目标做字典序选优——`min_energy` 先整数分电费再最大逐架暴露，`min_max_shelf_exposure` 先最大逐架暴露再电费。逐层状态数、枚举终点数、可行终点数和支配规则写入 `optimality_evidence`，可独立复核无更优计划；贪心"先冷却最热架"不被采用。

迟到真实读数经 `apply_feedback` 处理：已执行命令、已付电费与按真实结束温度发生的暴露全部冻结，绝不倒写；仅对未执行时段以真实温度为新起点重求剩余计划，生成递增 `plan_version`。缺读数报错而非按零处理，已超限暴露只增不减。无可行计划时整份建议状态为待核，附最低可达总暴露证据且不写任何命令。

`climate_plan` 与 `climate_command` 表持久化每时段命令（待执行/已执行/已作废）、设备占用、价格与计划版本；`climate_feedback` 留存读数 SHA256 与反馈链。`python -m shelfscan climate-report` 输出 JSON 与 Markdown 控制报告，普通 `report` 也包含温控表。相同输入稳定重算，源扫描与传感器 SHA 不受规划影响。CLI：`climate-register`、`climate-plan`、`climate-feedback`、`climate-report`。
