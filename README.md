# 馆藏盘点采录台

社区图书馆离线 RFID 盘点。冻结馆藏、借阅和读写器布局快照，导入本地扫描及借还日志，重建每册应在馆状态并输出差异和证据。仅用合成数据，不联网。

Python 3.11+，`python -m pip install -e .` 安装。`python -m shelfscan demo` 一条命令生成 SQLite 与 JSON/Markdown 报告，`python -m shelfscan --help` 查看独立命令，`python -m pytest -q` 运行测试。

Batch 指向 Copy，Reader 给出读写器架位和有效时间窗；Source 以原字节哈希去重，Event 以设备与序号幂等，重复扫描保留信号。Decision 结合借还时序、位置、信号强度给出在架、串架、缺失、借出或待核。多架信号接近时待核，不任意归位；借出册不判丢失。Issue 保留解析、身份、时序和定位错误，报告包含快照版本、来源哈希、事件 ID 和候选架位分数。批次状态为待接收、已校验、待核、已判定、作废，问题码由 issue.code 表示。

Run 记录重算生命周期；Evidence 将每条事件与来源、载荷哈希相连。导入和判定分别用事务保证原子性。Issue 与 Decision 历史按 run 累积，重算不删除旧批证据（如未知标签 `unexpected_copy`）。

## 捐赠接收与入藏

未知标签只登记素材、不自动入藏。`donation-demo`（或分步 CLI）驱动持久阶段：

1. `donations.register` 冻结未知标签原始读数、deed、封条与双签保管链为 `donation_case`。
2. `accession.verify` 实物核对闸门：标签/证据身份、既有 copy 碰撞、捐赠人签署、授权版本、封条完整与连续双签保管；任一失败进入“争议”并保留争议码，禁止确认。
3. `accession.confirm` 以同一 deed 版本+当前保管人+封条+标签身份不可分割地确认法律路径，条件更新保证并发双馆员只有一人成功；授权在确认前可撤回。
4. `accession.register_rights` 登记权属 holding；`build_snapshot` 生成下一批快照——无条件赠与须编目写入 copy 后才普通流通，一年期寄存不产生 copy、仅限阅览室并记录 2031-06-01 返还义务；`issue_voucher` 出具赠与证明或寄存返还单（案件唯一，互斥）。
5. 每个阶段独立持久、幂等可续跑；确认后编目失败停在“权属已登记”可原地重试，或 `compensate` 补偿回退；INV-G 的历史 `unexpected_copy` 与 stage_log 永不消除。

CLI：`donation-register / donation-verify / authorization-revoke / donation-confirm / donation-rights / donation-snapshot / donation-voucher / donation-compensate / donation-status`，`python -m shelfscan donation-demo --path {outright_gift,long_term_deposit}` 端到端演示。报告新增产权人、流通资格、返还义务、凭证与旧批证据章节。
