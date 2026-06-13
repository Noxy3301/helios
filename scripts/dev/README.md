# scripts/dev — 計測ランブック(phase16 で /tmp から救出)

- `sf1_milestone.sh` — SF=1 フル計測: fresh stack → helios load → InnoDB ref(3308) reload → matrix → RSS → md5。
- `plan_stability_test.sh` — plan 決定性検証: 2 cycle × (load → q17/18/20 → full matrix → 再計測)。
- `oltp_regression.sh` — TPC-C / TATP autogen 回帰(gates OFF のデフォルト経路、terminals=1)。

## 計測 env(標準)
- mysqld: `HELIOS_OPT_STATS=1 HELIOS_COST_V2=1 HELIOS_AGG_PUSHDOWN=1 HELIOS_ENABLE_SEMIJOIN=1`
- server: `HELIOS_PARALLEL_SERVER=1 HELIOS_PARALLEL_SERVER_SCAN=1`
- 接続後: `SET GLOBAL lineairdb_prefetch_execution=ON; SET GLOBAL lineairdb_prefetch_ro_novalidate=ON;`

## 罠(必読)
- lineairdb-server はインメモリ: 再起動でデータ消失。**load の前は必ず server 再起動**(DROP 残渣)。
- `pkill -x mysqld` は 3308 の InnoDB 参照系を巻き込む。**必ず pid file で kill**。
- SF=0.1 反復スクリプトで 3308 ref を上書きしない(md5 全 MISMATCH 誤報の前科 2 回)。
- 計測中にホスト/同居プロセスの CPU 占有がないこと(load 時間が ~29s@16core から大きくズレたら環境を疑う)。
