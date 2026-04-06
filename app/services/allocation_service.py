"""
dispatch_planner.py

简化调度计划模型

业务逻辑：
  1. 从合同总量计算需要发货的总车数（合同总量 ÷ TONS_PER_TRUCK，向上取整）
  2. 合同有效期约束：只在 [contract_date, end_date] ∩ 规划窗口 内排布发货
  3. 各仓库每日发货能力上限（最大车数）
  4. 目标：均匀到货——各冶炼厂每日到货车数方差最小（转化为线性约束）
  5. 输出：{仓库: {冶炼厂: {日期: 车数}}}

模型（LP）：
  变量：
    x[w, s, d]  >= 0, integer  —— 仓库 w 在第 d 天向冶炼厂 s 发出的车数
    dev_plus[s, d]  >= 0       —— 当天实际分配超出均值的量（松弛变量）
    dev_minus[s, d] >= 0       —— 当天实际分配低于均值的量（松弛变量）

  约束：
    (1) 合同总量约束：  sum_{w,d} x[w,s,d] == demand[s]      for each smelter s
    (2) 仓库日产能：    sum_s x[w,s,d] <= daily_cap[w]（若 daily_cap[w] 为 None 则不加此约束）
    (3) 合同有效期：    x[w,s,d] == 0  if d ∉ valid_dates[s]
    (4) 均匀偏差定义：  sum_w x[w,s,d] - target[s] == dev_plus[s,d] - dev_minus[s,d]

  目标：
    minimize  sum_{s,d} (dev_plus[s,d] + dev_minus[s,d])
"""

from __future__ import annotations

import math
import os
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
try:
    import pulp
except ImportError as e:
    raise ImportError("缺少依赖 pulp，请先安装：pip install pulp") from e


TONS_PER_TRUCK: int = 35  # 每车吨数


# ─────────────────────────────────────────────────────────
# 数据库操作
# ─────────────────────────────────────────────────────────

def get_active_contracts(as_of_date: str = None) -> List[ContractDemand]:
    """
    从数据库读取所有生效中的合同(包含已发车数量调整)

    参数:
        as_of_date: 截至日期 "YYYY-MM-DD",如果为None则使用当前时间

    返回:
        合同需求列表(已扣除截至指定时间的已发车数)
    """
    from app.services.contract_service import get_conn

    contracts = []
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 获取所有生效中的合同
                cur.execute("""
                    SELECT contract_no, contract_date, end_date, smelter_company,
                           total_quantity, truck_count
                    FROM pd_contracts
                    WHERE status = '生效中'
                    ORDER BY contract_date
                """)
                rows = cur.fetchall()

                for row in rows:
                    # 处理字典或元组
                    if isinstance(row, dict):
                        contract_no = row['contract_no']
                        contract_date = row['contract_date']
                        end_date = row['end_date']
                        smelter_company = row['smelter_company']
                        total_quantity = row['total_quantity']
                        truck_count = row.get('truck_count')
                    else:
                        contract_no = row[0]
                        contract_date = row[1]
                        end_date = row[2]
                        smelter_company = row[3]
                        total_quantity = row[4]
                        truck_count = row[5] if len(row) > 5 else None

                    contract_date = contract_date.strftime('%Y-%m-%d') if contract_date else None
                    end_date = end_date.strftime('%Y-%m-%d') if end_date else None
                    total_quantity = float(total_quantity) if total_quantity else 0

                    # 计算总车数
                    if truck_count:
                        total_trucks = int(truck_count)
                    else:
                        total_trucks = math.ceil(total_quantity / TONS_PER_TRUCK)

                    # 计算已发车数(截至指定时间点)
                    delivered_trucks = _get_delivered_truck_count(contract_no, as_of_date)
                    remaining_trucks = max(0, total_trucks - delivered_trucks)

                    # 更新总吨数(按剩余车数换算)
                    remaining_tons = remaining_trucks * TONS_PER_TRUCK

                    if contract_date and end_date:
                        contracts.append(ContractDemand(
                            contract_no=contract_no,
                            smelter=smelter_company,
                            total_tons=remaining_tons,
                            start_date=contract_date,
                            end_date=end_date
                        ))
    except Exception as e:
        print(f"读取合同数据失败: {e}")

    return contracts


def _get_delivered_truck_count(contract_no: str, as_of_date: str = None) -> int:
    """
    计算合同截至指定时间点的已发车数(从报单和磅单统计)

    参数:
        contract_no: 合同编号
        as_of_date: 截至日期 "YYYY-MM-DD",如果为None则使用当前时间

    返回:
        已发车数量
    """
    from app.services.contract_service import get_conn

    delivered_count = 0
    date_filter = ""

    if as_of_date:
        date_filter = f" AND created_at <= '{as_of_date} 23:59:59'"

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 1. 从报单统计已发车数
                query1 = f"""
                    SELECT COUNT(*) as count
                    FROM pd_deliveries
                    WHERE contract_no = %s
                      AND status IN ('已发货', '已装车', '在途', '已签收')
                      {date_filter}
                """
                cur.execute(query1, (contract_no,))
                row = cur.fetchone()
                if row:
                    # 处理字典或元组
                    count = row['count'] if isinstance(row, dict) else row[0]
                    delivered_count += int(count)

                # 2. 从磅单统计已发车数(避免重复统计,只统计没有关联报单的磅单)
                # 由于pd_deliveries表没有weighbill_id字段,这里只统计磅单
                query2 = f"""
                    SELECT COUNT(*) as count
                    FROM pd_weighbills wb
                    WHERE wb.contract_no = %s
                      {date_filter}
                """
                cur.execute(query2, (contract_no,))
                row = cur.fetchone()
                if row:
                    count = row['count'] if isinstance(row, dict) else row[0]
                    delivered_count += int(count)

    except Exception as e:
        print(f"计算已发车数失败 {contract_no}: {e}")

    return delivered_count


def get_warehouses() -> List[str]:
    """
    从数据库读取所有仓库的大区经理

    返回:
        大区经理列表
    """
    from app.services.contract_service import get_conn

    managers = []
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COALESCE(regional_manager, warehouse_name) as manager
                    FROM pd_warehouses
                    WHERE is_active = 1 OR is_active IS NULL
                    ORDER BY warehouse_name
                """)
                rows = cur.fetchall()

                for row in rows:
                    manager = row['manager'] if isinstance(row, dict) else row[0]
                    managers.append(manager)
    except Exception as e:
        print(f"读取仓库数据失败: {e}")

    return managers


def _per_warehouse_daily_truck_cap() -> Optional[int]:
    """
    每库每日车数上限。
    - 未设置、为空、0 或非法值：返回 None，表示**不封顶**（线性规划中**不添加**仓库日产能约束）。
    - 设置 ALLOCATION_DAILY_CAP_PER_WAREHOUSE 为正整数时启用封顶（例如 50）。
    """
    raw = (os.getenv("ALLOCATION_DAILY_CAP_PER_WAREHOUSE") or "").strip()
    if not raw:
        return None
    try:
        n = int(raw)
    except ValueError:
        return None
    if n <= 0:
        return None
    return n


def get_warehouse_daily_capacity() -> Dict[str, Optional[int]]:
    """
    获取各仓库每日发货能力上限。

    默认**不封顶**（值为 None；排产模型中不写「每日 ≤ cap」约束）。
    设置环境变量 ALLOCATION_DAILY_CAP_PER_WAREHOUSE 为正整数后，各库使用该上限。
    """
    cap = _per_warehouse_daily_truck_cap()
    warehouses = get_warehouses()

    daily_cap: Dict[str, Optional[int]] = {warehouse: cap for warehouse in warehouses}

    if not daily_cap:
        daily_cap = {"仓库A": cap, "仓库B": cap, "仓库C": cap}

    return daily_cap


def save_predictions_to_db(plan: dict, prediction_date: str, is_test: bool = False):
    """保存预测结果到数据库"""
    from app.services.contract_service import get_conn

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM pd_allocation_predictions WHERE prediction_date = %s", (prediction_date,))

            # 获取大区经理到仓库的映射
            cur.execute("SELECT COALESCE(regional_manager, warehouse_name) as manager, warehouse_name FROM pd_warehouses")
            manager_to_warehouse = {row[0]: row[1] for row in cur.fetchall()}

            for manager, contracts in plan.items():
                warehouse = manager_to_warehouse.get(manager, manager)
                for contract_no, smelters in contracts.items():
                    for smelter, dates in smelters.items():
                        for date, truck_count in dates.items():
                            cur.execute("""
                                INSERT INTO pd_allocation_predictions
                                (prediction_date, warehouse_name, regional_manager, contract_no,
                                 smelter_company, delivery_date, truck_count, is_test)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            """, (prediction_date, warehouse, manager, contract_no,
                                  smelter, date, truck_count, 1 if is_test else 0))


def get_predictions(regional_managers=None, smelters=None, contract_nos=None):
    """查询预测结果"""
    from app.services.contract_service import get_conn

    filters = []
    params = []

    if regional_managers:
        placeholders = ','.join(['%s'] * len(regional_managers))
        filters.append(f"regional_manager IN ({placeholders})")
        params.extend(regional_managers)

    if smelters:
        placeholders = ','.join(['%s'] * len(smelters))
        filters.append(f"smelter_company IN ({placeholders})")
        params.extend(smelters)

    if contract_nos:
        placeholders = ','.join(['%s'] * len(contract_nos))
        filters.append(f"contract_no IN ({placeholders})")
        params.extend(contract_nos)

    where_clause = " AND ".join(filters) if filters else "1=1"

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(prediction_date) FROM pd_allocation_predictions")
            latest_date = cur.fetchone()[0]

            if not latest_date:
                return {}, None, 0

            query = f"""
                SELECT regional_manager, contract_no, smelter_company,
                       delivery_date, truck_count
                FROM pd_allocation_predictions
                WHERE prediction_date = %s AND {where_clause}
                ORDER BY regional_manager, contract_no, delivery_date
            """
            cur.execute(query, [latest_date] + params)
            rows = cur.fetchall()

            predictions = {}
            total_trucks = 0

            for row in rows:
                manager = row[0] or '未分配'
                contract = row[1]
                smelter = row[2]
                date = str(row[3])
                trucks = row[4]

                if manager not in predictions:
                    predictions[manager] = {}
                if contract not in predictions[manager]:
                    predictions[manager][contract] = {}
                if smelter not in predictions[manager][contract]:
                    predictions[manager][contract][smelter] = {}

                predictions[manager][contract][smelter][date] = trucks
                total_trucks += trucks

            return predictions, str(latest_date), total_trucks


def get_filter_options():
    """获取筛选选项"""
    from app.services.contract_service import get_conn

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT COALESCE(regional_manager, warehouse_name) as manager
                FROM pd_warehouses
                WHERE is_active = 1
                ORDER BY manager
            """)
            managers = [row[0] for row in cur.fetchall()]

            cur.execute("""
                SELECT DISTINCT smelter_company
                FROM pd_contracts
                WHERE status = '生效中'
                ORDER BY smelter_company
            """)
            smelters = [row[0] for row in cur.fetchall()]

            cur.execute("""
                SELECT DISTINCT contract_no
                FROM pd_contracts
                WHERE status = '生效中'
                ORDER BY contract_no
            """)
            contracts = [row[0] for row in cur.fetchall()]

            return managers, smelters, contracts


def get_active_warehouse_names() -> List[str]:
    """启用仓库的 `warehouse_name` 列表（下拉与 plan 第一层 key）。"""
    from app.services.contract_service import get_conn

    names: List[str] = []
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT warehouse_name
                    FROM pd_warehouses
                    WHERE is_active = 1 OR is_active IS NULL
                    ORDER BY warehouse_name
                    """
                )
                for row in cur.fetchall() or []:
                    wn = row["warehouse_name"] if isinstance(row, dict) else row[0]
                    if wn:
                        names.append(str(wn))
    except Exception as e:
        print(f"读取仓库名称列表失败: {e}")
    return names


def get_warehouse_names_for_user(user: Optional[Dict[str, Any]]) -> List[str]:
    """
    当前用户可见仓库（下拉 warehouse_options）。
    管理员：全部启用库；大区经理：`pd_warehouses.regional_manager` 与本人姓名一致；其余角色：全部启用库。
    """
    if not user:
        return get_active_warehouse_names()
    role = (user.get("role") or "").strip()
    if role == "管理员":
        return get_active_warehouse_names()
    if role == "大区经理":
        name = (user.get("name") or "").strip()
        if not name:
            return []
        from app.services.contract_service import get_conn

        names: List[str] = []
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT warehouse_name
                        FROM pd_warehouses
                        WHERE warehouse_name IS NOT NULL AND TRIM(warehouse_name) <> ''
                          AND (is_active = 1 OR is_active IS NULL)
                          AND TRIM(COALESCE(regional_manager, '')) = %s
                        ORDER BY warehouse_name
                        """,
                        (name,),
                    )
                    for row in cur.fetchall() or []:
                        wn = row["warehouse_name"] if isinstance(row, dict) else row[0]
                        if wn:
                            names.append(str(wn))
        except Exception as e:
            print(f"按大区经理读取仓库失败: {e}")
        return names
    return get_active_warehouse_names()


def get_warehouse_names_by_ids(warehouse_ids: List[int]) -> List[str]:
    """按主键解析 `pd_warehouses.warehouse_name`，仅包含存在的 id。"""
    ids = [i for i in warehouse_ids if isinstance(i, int) and i > 0]
    if not ids:
        return []
    from app.services.contract_service import get_conn

    placeholders = ",".join(["%s"] * len(ids))
    names: List[str] = []
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT warehouse_name
                    FROM pd_warehouses
                    WHERE id IN ({placeholders})
                    ORDER BY warehouse_name
                    """,
                    ids,
                )
                for row in cur.fetchall() or []:
                    wn = row["warehouse_name"] if isinstance(row, dict) else row[0]
                    if wn:
                        names.append(str(wn))
    except Exception as e:
        print(f"按 id 解析仓库名称失败: {e}")
    return names


def _coerce_db_date(v: Any) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    s = str(v).strip()[:10]
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None


def _scalar_cell(row: Any) -> Any:
    if not row:
        return None
    if isinstance(row, dict):
        return next(iter(row.values())) if row else None
    return row[0]


def _prediction_agg_row(row: Any) -> Tuple[Any, Any, Any, Any, Any]:
    """聚合查询一行：兼容元组游标与 DictCursor。"""
    if isinstance(row, dict):
        return (
            row.get("wh"),
            row.get("contract_no"),
            row.get("smelter_company"),
            row.get("dday"),
            row.get("tc"),
        )
    return row[0], row[1], row[2], row[3], row[4]


def _min_max_delivery_row(row: Any) -> Tuple[Any, Any]:
    if not row:
        return None, None
    if isinstance(row, dict):
        vals = list(row.values())
        if len(vals) >= 2:
            return vals[0], vals[1]
        if len(vals) == 1:
            return vals[0], vals[0]
        return None, None
    return row[0], row[1]


def query_ai_purchase_quantity(
    start_date: str,
    end_date: str,
    *,
    warehouse: Optional[str] = None,
    warehouse_names: Optional[List[str]] = None,
    contract_no: Optional[str] = None,
    smelter: Optional[str] = None,
    max_days: int = 30,
    current_user: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    统一查询：仓库下拉选项 + 预测 plan（仓库→合同→冶炼厂→日期→车数）。
    使用 `pd_allocation_predictions` 中最近一次 `prediction_date` 的全量快照，
    再按 `delivery_date` 落在 [start_date, end_date] 内筛选。
    若请求区间与快照内实际发货日无交集，会自动用「交集区间」再查一次，避免 plan 全空（仍按请求区间补全每日键，无数据为 0）。
    大区经理仅可见本人名下仓库及 `regional_manager` 匹配的行；`warehouse` 须在可见列表内。
    """
    from app.services.contract_service import get_conn

    def _fail(msg: str, status_code: int = 400) -> Dict[str, Any]:
        return {"success": False, "message": msg, "data": None, "status_code": status_code}

    warehouse_options = get_warehouse_names_for_user(current_user)

    sd = (start_date or "").strip()
    ed = (end_date or "").strip()
    if not sd or not ed:
        return _fail("start_date 与 end_date 均为必填")

    try:
        d_start = datetime.strptime(sd, "%Y-%m-%d").date()
        d_end = datetime.strptime(ed, "%Y-%m-%d").date()
    except ValueError:
        return _fail("日期格式须为 YYYY-MM-DD")

    if d_end < d_start:
        return _fail("end_date 不能早于 start_date")

    if (d_end - d_start).days + 1 > max_days:
        return _fail(f"查询区间不能超过 {max_days} 天")

    wh_filter = (warehouse or "").strip() or None
    wh_list = [w.strip() for w in (warehouse_names or []) if w and str(w).strip()]
    cn_filter = (contract_no or "").strip() or None
    sm_filter = (smelter or "").strip() or None

    if wh_filter and current_user and (current_user.get("role") or "").strip() == "大区经理":
        if wh_filter not in warehouse_options:
            return _fail("无权筛选该仓库或该仓库不在您的负责范围", 403)

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(prediction_date) FROM pd_allocation_predictions")
                latest_pd = _scalar_cell(cur.fetchone())
    except Exception as e:
        return _fail(f"查询预测数据失败: {e}", 500)

    if not latest_pd:
        return {
            "success": True,
            "message": (
                "表 pd_allocation_predictions 中尚无预测快照，故 plan 为空。"
                "请先调用「生成调度分配计划」接口写入数据：GET /api/v1/allocation/plan"
                "（无需登录；可选参数 window_start、H、as_of_date）。"
                "成功后再调用本查询接口即可返回 plan。"
            ),
            "data": {
                "warehouse_options": warehouse_options,
                "plan": {},
            },
        }

    if hasattr(latest_pd, "strftime"):
        latest_pd_str = latest_pd.strftime("%Y-%m-%d")
    else:
        latest_pd_str = str(latest_pd)[:10]

    base_conditions: List[str] = ["prediction_date = %s"]
    base_params: List[Any] = [latest_pd_str]

    if current_user and (current_user.get("role") or "").strip() == "大区经理":
        mgr_name = (current_user.get("name") or "").strip()
        opts = warehouse_options
        if not mgr_name:
            base_conditions.append("1=0")
        elif opts:
            ph = ",".join(["%s"] * len(opts))
            base_conditions.append(
                f"(warehouse_name IN ({ph}) OR TRIM(COALESCE(regional_manager, '')) = %s)"
            )
            base_params.extend(opts)
            base_params.append(mgr_name)
        else:
            base_conditions.append("TRIM(COALESCE(regional_manager, '')) = %s")
            base_params.append(mgr_name)

    if wh_list:
        ph = ",".join(["%s"] * len(wh_list))
        base_conditions.append(f"warehouse_name IN ({ph})")
        base_params.extend(wh_list)
    elif wh_filter:
        base_conditions.append("warehouse_name = %s")
        base_params.append(wh_filter)
    if cn_filter:
        base_conditions.append("contract_no LIKE %s")
        base_params.append(f"%{cn_filter}%")
    if sm_filter:
        base_conditions.append("smelter_company LIKE %s")
        base_params.append(f"%{sm_filter}%")

    wh_expr = "COALESCE(NULLIF(TRIM(warehouse_name), ''), regional_manager, '未分配')"

    def _fetch_aggregate(lo: str, hi: str) -> List[Any]:
        conditions = base_conditions + ["delivery_date >= %s", "delivery_date <= %s"]
        params = base_params + [lo, hi]
        sql = f"""
            SELECT
                {wh_expr} AS wh,
                contract_no,
                smelter_company,
                DATE(delivery_date) AS dday,
                SUM(truck_count) AS tc
            FROM pd_allocation_predictions
            WHERE {' AND '.join(conditions)}
            GROUP BY {wh_expr}, contract_no, smelter_company, DATE(delivery_date)
            ORDER BY wh, contract_no, smelter_company, dday
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return list(cur.fetchall() or [])

    hint_msg = ""
    try:
        rows = _fetch_aggregate(sd, ed)
    except Exception as e:
        return _fail(f"查询预测明细失败: {e}", 500)

    if not rows:
        bound_sql = f"""
            SELECT MIN(delivery_date), MAX(delivery_date)
            FROM pd_allocation_predictions
            WHERE {' AND '.join(base_conditions)}
        """
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(bound_sql, base_params)
                    brow = cur.fetchone()
        except Exception as e:
            return _fail(f"查询预测日期范围失败: {e}", 500)

        dmin_raw, dmax_raw = _min_max_delivery_row(brow)
        dmin = _coerce_db_date(dmin_raw)
        dmax = _coerce_db_date(dmax_raw)

        if dmin is None or dmax is None:
            return {
                "success": True,
                "message": "当前预测快照下无明细（请检查仓库/合同/冶炼厂筛选是否过严，或先生成调度计划）",
                "data": {
                    "warehouse_options": warehouse_options,
                    "plan": {},
                },
            }

        eff_lo = max(d_start, dmin)
        eff_hi = min(d_end, dmax)
        if eff_lo > eff_hi:
            return {
                "success": True,
                "message": (
                    f"预测快照({latest_pd_str})内发货日为 {dmin.isoformat()}～{dmax.isoformat()}，"
                    f"与查询区间 {sd}～{ed} 无交集；请将起止日期包含该区间后重试"
                ),
                "data": {
                    "warehouse_options": warehouse_options,
                    "plan": {},
                },
            }

        lo_s, hi_s = eff_lo.strftime("%Y-%m-%d"), eff_hi.strftime("%Y-%m-%d")
        try:
            rows = _fetch_aggregate(lo_s, hi_s)
        except Exception as e:
            return _fail(f"查询预测明细失败: {e}", 500)

        if not rows:
            return {
                "success": True,
                "message": (
                    f"快照发货日 {dmin.isoformat()}～{dmax.isoformat()} 与查询区间有交集，"
                    "但聚合结果为空，请检查仓库/合同/冶炼厂筛选或数据是否完整"
                ),
                "data": {
                    "warehouse_options": warehouse_options,
                    "plan": {},
                },
            }

        hint_msg = (
            f"查询区间与快照发货日已取交集：实际使用 {lo_s}～{hi_s} "
            f"（快照内发货日范围为 {dmin.isoformat()}～{dmax.isoformat()}）"
        )

    plan: Dict[str, Any] = {}
    for row in rows:
        w_h, c_n, s_m, d_day, t_c = _prediction_agg_row(row)
        if w_h is None or c_n is None or s_m is None:
            continue
        if hasattr(d_day, "strftime"):
            d_str = d_day.strftime("%Y-%m-%d")
        else:
            d_str = str(d_day)[:10]
        plan.setdefault(w_h, {}).setdefault(c_n, {}).setdefault(s_m, {})[d_str] = int(t_c or 0)

    # 统一输出：每个「仓库→合同→冶炼厂」下，请求区间内每日均有键，无数据为 0
    date_keys = _date_range(sd, ed)
    for contracts in plan.values():
        for smelters in contracts.values():
            for sm_key in list(smelters.keys()):
                day_map = smelters[sm_key]
                smelters[sm_key] = {
                    dk: int(day_map.get(dk, 0) or 0) for dk in date_keys
                }

    return {
        "success": True,
        "message": hint_msg,
        "data": {
            "warehouse_options": warehouse_options,
            "plan": plan,
        },
    }


# ─────────────────────────────────────────────────────────
# 数据结构
# ─────────────────────────────────────────────────────────

class ContractDemand:
    """单个合同的调度需求"""
    def __init__(
        self,
        contract_no: str,
        smelter: str,           # 冶炼厂名称
        total_tons: float,      # 合同总量（吨）
        start_date: str,        # 合同开始日期 "YYYY-MM-DD"
        end_date: str,          # 合同截止日期 "YYYY-MM-DD"
    ):
        self.contract_no = contract_no
        self.smelter = smelter
        self.total_tons = total_tons
        self.start_date = start_date
        self.end_date = end_date
        # 总需求车数（向上取整）
        self.total_trucks: int = math.ceil(total_tons / TONS_PER_TRUCK)


# ─────────────────────────────────────────────────────────
# 辅助函数
# ─────────────────────────────────────────────────────────

def _date_range(start: str, end: str) -> List[str]:
    """生成 [start, end] 闭区间的日期列表"""
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    result = []
    cur = s
    while cur <= e:
        result.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return result


def _intersect_dates(dates_a: List[str], dates_b: List[str]) -> List[str]:
    """返回两个日期列表的交集（保持顺序）"""
    set_b = set(dates_b)
    return [d for d in dates_a if d in set_b]


# ─────────────────────────────────────────────────────────
# 核心求解函数
# ─────────────────────────────────────────────────────────

def solve_dispatch_plan(
    contracts: List[ContractDemand],
    warehouses: List[str],
    daily_cap: Dict[str, Optional[int]],  # {仓库: 每日最大车数；None 表示不封顶}
    window_start: str,              # 规划窗口开始 "YYYY-MM-DD"
    window_end: str,                # 规划窗口结束 "YYYY-MM-DD"
    solver_msg: bool = False,
) -> Tuple[Dict[str, Dict[str, Dict[str, int]]], str]:
    """
    求解均匀到货调度计划。

    参数：
        contracts    : 合同需求列表
        warehouses   : 仓库名称列表
        daily_cap    : 各仓库每日发货能力；值为 None 时不加日上限约束
        window_start : 规划窗口起始日期
        window_end   : 规划窗口结束日期
        solver_msg   : 是否打印求解器日志

    返回：
        (plan, status)
        plan   : {仓库: {冶炼厂: {日期: 车数}}}，仅包含 > 0 的条目
        status : "Optimal" / "Infeasible" / ...
    """
    window_dates = _date_range(window_start, window_end)

    # ── 按冶炼厂合并同一冶炼厂的多份合同，各自保留有效期（一个合同 = 一个调度单元）
    #    key = (contract_no, smelter)，不合并，以便各合同独立约束有效期
    units = contracts  # 直接用合同列表，每个合同独立

    # 过滤：有效期与窗口无交集的合同跳过
    def valid_dates_for(c: ContractDemand) -> List[str]:
        contract_dates = _date_range(c.start_date, c.end_date)
        return _intersect_dates(window_dates, contract_dates)

    active_units = [(c, valid_dates_for(c)) for c in units]
    active_units = [(c, vd) for c, vd in active_units if vd]  # 去掉无交集的

    if not active_units:
        return {}, "NoActiveContracts"

    prob = pulp.LpProblem("dispatch_plan", pulp.LpMinimize)

    # ── 决策变量 x[w, cid, d] ──
    x: Dict[Tuple[str, str, str], pulp.LpVariable] = {}
    for c, vd in active_units:
        for w in warehouses:
            for d in vd:
                key = (w, c.contract_no, d)
                x[key] = pulp.LpVariable(
                    f"x_{w}_{c.contract_no}_{d}", lowBound=0, cat="Integer"
                )

    # ── 松弛变量（均匀偏差） dev_plus / dev_minus[cid, d] ──
    dev_plus:  Dict[Tuple[str, str], pulp.LpVariable] = {}
    dev_minus: Dict[Tuple[str, str], pulp.LpVariable] = {}
    for c, vd in active_units:
        for d in vd:
            dev_plus[(c.contract_no, d)]  = pulp.LpVariable(
                f"dp_{c.contract_no}_{d}", lowBound=0
            )
            dev_minus[(c.contract_no, d)] = pulp.LpVariable(
                f"dm_{c.contract_no}_{d}", lowBound=0
            )

    # ── 目标：最小化总偏差（均匀到货） ──
    prob += pulp.lpSum(
        dev_plus[(c.contract_no, d)] + dev_minus[(c.contract_no, d)]
        for c, vd in active_units
        for d in vd
    )

    # ── 约束 1：每个合同在窗口内的发货总量 == 合同需求车数 ──
    #    （若仓库产能不足，此处改为 <= 并接受欠发；若希望强制完成则保留 ==）
    for c, vd in active_units:
        prob += (
            pulp.lpSum(x[(w, c.contract_no, d)] for w in warehouses for d in vd)
            == c.total_trucks,
            f"demand_{c.contract_no}",
        )

    # ── 约束 2：仓库每日总发货 <= 产能上限（None 表示不封顶，不添加约束）──
    for w in warehouses:
        cap = daily_cap.get(w)
        if cap is None:
            continue
        for d in window_dates:
            # 当天该仓库有哪些合同可发
            active_on_day = [
                x[(w, c.contract_no, d)]
                for c, vd in active_units
                if d in vd
            ]
            if active_on_day:
                prob += (
                    pulp.lpSum(active_on_day) <= cap,
                    f"cap_{w}_{d}",
                )

    # ── 约束 3：均匀偏差定义 ──
    #    每合同每天实际发货（所有仓库之和）与均匀目标的偏差
    for c, vd in active_units:
        n_days = len(vd)
        # 均匀目标：总需求 / 有效天数（允许是小数，偏差松弛处理）
        target = c.total_trucks / n_days
        for d in vd:
            daily_total = pulp.lpSum(x[(w, c.contract_no, d)] for w in warehouses)
            prob += (
                daily_total - target == dev_plus[(c.contract_no, d)] - dev_minus[(c.contract_no, d)],
                f"dev_{c.contract_no}_{d}",
            )

    # ── 求解 ──
    solver = pulp.PULP_CBC_CMD(msg=1 if solver_msg else 0)
    prob.solve(solver)
    status = pulp.LpStatus[prob.status]

    if status not in ("Optimal", "Feasible"):
        return {}, status

    # ── 提取结果 ──
    plan: Dict[str, Dict[str, Dict[str, int]]] = {}
    for (w, cno, d), var in x.items():
        val = int(round(var.varValue or 0))
        if val <= 0:
            continue
        # 找回 smelter 名称
        smelter = next(c.smelter for c, _ in active_units if c.contract_no == cno)
        # 输出格式: {仓库: {合同编号: {冶炼厂: {日期: 车数}}}}
        plan.setdefault(w, {}).setdefault(cno, {}).setdefault(smelter, {})[d] = val

    return plan, status


# ─────────────────────────────────────────────────────────
# 大区经理每日分配需求（合同有效期 ∩ 报货计划 + 订货计划均分）
# ─────────────────────────────────────────────────────────

_GRACE_DAYS_NO_END_DATE = 4


def _contract_window_for_delivery_plan(delivery_plan_id: int) -> Optional[Tuple[date, date]]:
    """
    同一报货计划下所有「生效中」合同的生效起止（起取最早签订日，止取最晚截止日；
    无 end_date 时按签订日 + 4 天截止，与合同失效逻辑一致）。
    """
    from app.services.contract_service import get_conn
    from pymysql.cursors import DictCursor

    try:
        with get_conn() as conn:
            with conn.cursor(DictCursor) as cur:
                cur.execute(
                    """
                    SELECT contract_date, end_date
                    FROM pd_contracts
                    WHERE delivery_plan_id = %s AND status = '生效中'
                    """,
                    (delivery_plan_id,),
                )
                rows = cur.fetchall() or []
    except Exception:
        return None

    if not rows:
        return None

    starts: List[date] = []
    ends: List[date] = []
    for r in rows:
        cd = r.get("contract_date")
        ed = r.get("end_date")
        if hasattr(cd, "date"):
            cd = cd.date()
        if hasattr(ed, "date"):
            ed = ed.date()
        if cd is None:
            continue
        starts.append(cd)
        if ed is not None:
            ends.append(ed)
        else:
            ends.append(cd + timedelta(days=_GRACE_DAYS_NO_END_DATE))

    if not starts or not ends:
        return None

    return min(starts), max(ends)


def _delivered_trucks_for_order_plan(order_plan_id: int) -> int:
    """订货计划已发车辆（报单统计，与合同已发车口径一致）。"""
    from app.services.contract_service import get_conn

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*) AS n
                    FROM pd_deliveries
                    WHERE order_plan_id = %s
                      AND status IN ('已发货', '已装车', '在途', '已签收')
                    """,
                    (order_plan_id,),
                )
                row = cur.fetchone()
                if not row:
                    return 0
                n = row["n"] if isinstance(row, dict) else row[0]
                return int(n or 0)
    except Exception:
        return 0


def _spread_integer_total(total: int, num_slots: int) -> List[int]:
    """将 total 均分到 num_slots 份，前 remainder 份多 1。"""
    if num_slots <= 0:
        return []
    if total <= 0:
        return [0] * num_slots
    base = total // num_slots
    rem = total % num_slots
    return [base + (1 if i < rem else 0) for i in range(num_slots)]


def compute_manager_daily_allocation(
    *,
    today: Optional[date] = None,
) -> Dict[str, Any]:
    """
    基于生效中合同在报货计划上的生效期限，以及审核中/审核通过的订货计划车数，
    将剩余车数在有效日区间内均分，得到每位大区经理每日运货量（吨）。

    仅包含「当日及未来」的日期；无订货计划或有效天数为 0 的不产出条目。
    """
    from app.services.contract_service import get_conn
    from pymysql.cursors import DictCursor

    if today is None:
        today = datetime.now().date()

    # date_str -> manager_name -> { truck_count, tonnage, order_plan_ids }
    by_date_mgr: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(
        lambda: defaultdict(
            lambda: {"truck_count": 0, "tonnage": 0.0, "order_plan_ids": []}
        )
    )

    try:
        with get_conn() as conn:
            with conn.cursor(DictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        op.id AS order_plan_id,
                        op.delivery_plan_id,
                        op.truck_count,
                        op.created_by_name,
                        op.plan_no,
                        dp.plan_start_date,
                        dp.plan_status
                    FROM pd_order_plans op
                    INNER JOIN pd_delivery_plans dp ON dp.id = op.delivery_plan_id
                    WHERE op.audit_status IN ('待审核', '审核通过')
                      AND (dp.plan_status = '生效中' OR dp.plan_status IS NULL OR dp.plan_status = '')
                    ORDER BY op.id
                    """
                )
                order_rows = cur.fetchall() or []
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "tonnage_per_truck": TONS_PER_TRUCK,
            "days": [],
            "meta": {},
        }

    for row in order_rows:
        op_id = int(row["order_plan_id"])
        delivery_plan_id = int(row["delivery_plan_id"])
        truck_cap = int(row.get("truck_count") or 0)
        manager = (row.get("created_by_name") or "").strip() or "未指定"
        psd = row.get("plan_start_date")
        if hasattr(psd, "date"):
            psd = psd.date()

        window = _contract_window_for_delivery_plan(delivery_plan_id)
        if not window:
            continue
        eff_start, eff_end = window

        if psd is not None:
            eff_start = max(eff_start, psd)

        # 仅当日及未来
        window_start = max(today, eff_start)
        window_end = eff_end
        if window_start > window_end:
            continue

        date_list = _date_range(
            window_start.strftime("%Y-%m-%d"),
            window_end.strftime("%Y-%m-%d"),
        )
        # 再保险：只要 >= today
        date_list = [d for d in date_list if d >= today.strftime("%Y-%m-%d")]
        n_days = len(date_list)
        if n_days == 0 or truck_cap <= 0:
            continue

        delivered = _delivered_trucks_for_order_plan(op_id)
        remaining = max(0, truck_cap - delivered)
        if remaining <= 0:
            continue

        per_day = _spread_integer_total(remaining, n_days)
        for d_str, trucks in zip(date_list, per_day):
            if trucks <= 0:
                continue
            slot = by_date_mgr[d_str][manager]
            slot["truck_count"] += trucks
            slot["tonnage"] = round(float(slot["truck_count"]) * TONS_PER_TRUCK, 3)
            if op_id not in slot["order_plan_ids"]:
                slot["order_plan_ids"].append(op_id)

    # 组装 days 列表（按日期排序）
    sorted_dates = sorted(by_date_mgr.keys())
    days_out: List[Dict[str, Any]] = []
    for d in sorted_dates:
        mgr_map = by_date_mgr[d]
        by_manager: List[Dict[str, Any]] = []
        total_trucks = 0
        total_tonnage = 0.0
        for name in sorted(mgr_map.keys()):
            info = mgr_map[name]
            tc = int(info["truck_count"])
            tn = float(info["tonnage"])
            total_trucks += tc
            total_tonnage += tn
            by_manager.append(
                {
                    "manager_name": name,
                    "truck_count": tc,
                    "tonnage": round(tn, 3),
                    "order_plan_ids": sorted(info["order_plan_ids"]),
                }
            )
        days_out.append(
            {
                "date": d,
                "by_manager": by_manager,
                "total_trucks": total_trucks,
                "total_tonnage": round(total_tonnage, 3),
            }
        )

    return {
        "success": True,
        "tonnage_per_truck": TONS_PER_TRUCK,
        "days": days_out,
        "meta": {
            "today": today.isoformat(),
            "date_count": len(days_out),
        },
    }


