# 馆藏盘点采录台

社区图书馆离线 RFID 盘点。冻结馆藏、借阅和读写器布局快照，导入本地扫描及借还日志，重建每册应在馆状态并输出差异和证据。仅用合成数据，不联网。

Python 3.11+，`python -m pip install -e .` 安装。`python -m shelfscan demo` 一条命令生成 SQLite 与 JSON/Markdown 报告，`python -m shelfscan --help` 查看独立命令，`python -m pytest -q` 运行测试。

Batch 指向 Copy，Reader 给出读写器架位和有效时间窗；Source 以原字节哈希去重，Event 以设备与序号幂等，重复扫描保留信号。Decision 结合借还时序、位置、信号强度给出在架、串架、缺失、借出或待核。多架信号接近时待核，不任意归位；借出册不判丢失。Issue 保留解析、身份、时序和定位错误，报告包含快照版本、来源哈希、事件 ID 和候选架位分数。批次状态为待接收、已校验、待核、已判定、作废，问题码由 issue.code 表示。

Run 记录重算生命周期；Evidence 将每条事件与来源、载荷哈希相连。导入和判定分别用事务保证原子性。

## 馆藏实体标签制发

`python -m shelfscan label demo --workdir out/demo` 执行整套实体制发流程，`python -m shelfscan label --help` 查看分步命令。

- **输入绑定**：`register` 冻结 `fixtures/label-case.json` 与 `copy` 快照、`event`/`source` 采录证据和 `rules.evaluate` 的架位结论，生成 copy 级 `subject_sha256`。架位、采录身份或模板变化都会改变摘要。
- **纸面字节**：`render_paper` 生成确定字节并落库为 BLOB；`public_call_number` 印 `SHELF S1/S2`、copy ID 与序号，`opaque_tag` 只印 `TOKEN TK-C1-A/TK-C2-B`、copy ID 与序号，架位需经馆内授权映射。一次 `open_job` 只执行一种合同，另一种版式只写入 `label_control` 作未执行对照（不分配序号）。
- **预留**：`reserve_copy` 以 `BEGIN IMMEDIATE` 串行化并发馆员，部分唯一索引保证一册至多一张有效标签，`label.serial` 主键保证序号全局只用一次。
- **打印**：`label_command.nonce` 唯一且不可重复；`PRN-1` 在独立 SQLite 中持久记录命令与 `emission`，出纸日志不可改写。重复 nonce 不会二次出纸；ACK 只表示命令得到回复，不能让标签变成 `applied`。
- **验扫**：`VERIFY-1` 回读贴在图书上的实物标签，比对序号、纸面摘要、所在 copy、模板版本和当前快照；`wrong_book`、`content_mismatch`、`no_paper`、`snapshot_stale` 都会保留证据且不置为 `applied`。
- **中断恢复**：`recover_job` 只认不可改写出纸日志与验扫记录。纸片核实贴对时沿用原序号；碎损须有销毁见证后作废并换新号补印；去向不明则隔离旧号并换新号。库回滚不会删除已出纸，`sync_device` 从设备日志重建镜像。
- **审计**：`label_evidence` 逐步记录预留、发送、出纸、ACK、验扫、作废、销毁、补印，并以 `previous_sha256` 串成证据链；`report` 同时输出 JSON 与 Markdown，列出当前有效序号、全部曾打印序号、隔离/销毁状态、实际纸面字节和未执行对照版式。

```bash
python -m shelfscan label demo --workdir out/label-public   # 输出实际纸面字节、证据链与未执行对照
python -m shelfscan label demo --workdir out/label-opaque --contract opaque_tag
python -m shelfscan label report --db out/label-public/shelfscan.db --out out/label-public/report.json --devices out/label-public
python -B -m pytest -q -p no:cacheprovider
```
