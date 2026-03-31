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
    (2) 仓库日产能：    sum_s x[w,s,d] <= daily_cap[w]       for each (w,d)
    (3) 合同有效期：    x[w,s,d] == 0  if d ∉ valid_dates[s]
    (4) 均匀偏差定义：  sum_w x[w,s,d] - target[s] == dev_plus[s,d] - dev_minus[s,d]

  目标：
    minimize  sum_{s,d} (dev_plus[s,d] + dev_minus[s,d])
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
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
    从数据库读取所有仓库

    返回:
        仓库名称列表
    """
    from app.services.contract_service import get_conn

    warehouses = []
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT warehouse_name
                    FROM pd_warehouses
                    WHERE is_active = 1 OR is_active IS NULL
                    ORDER BY warehouse_name
                """)
                rows = cur.fetchall()

                for row in rows:
                    # 处理字典或元组
                    warehouse_name = row['warehouse_name'] if isinstance(row, dict) else row[0]
                    warehouses.append(warehouse_name)
    except Exception as e:
        print(f"读取仓库数据失败: {e}")

    return warehouses


def get_warehouse_daily_capacity() -> Dict[str, int]:
    """
    获取各仓库每日发货能力上限(模拟数据)

    返回:
        {仓库名称: 每日最大车数}
    """
    # 读取仓库列表
    warehouses = get_warehouses()

    # 模拟数据:每个仓库每天最多发 10 车
    # 后续可以从数据库配置表读取
    daily_cap = {}
    for warehouse in warehouses:
        daily_cap[warehouse] = 10  # 模拟值

    # 如果没有仓库,返回默认值
    if not daily_cap:
        daily_cap = {
            "仓库A": 10,
            "仓库B": 10,
            "仓库C": 10
        }

    return daily_cap


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
    daily_cap: Dict[str, int],      # {仓库: 每日最大车数}
    window_start: str,              # 规划窗口开始 "YYYY-MM-DD"
    window_end: str,                # 规划窗口结束 "YYYY-MM-DD"
    solver_msg: bool = False,
) -> Tuple[Dict[str, Dict[str, Dict[str, int]]], str]:
    """
    求解均匀到货调度计划。

    参数：
        contracts    : 合同需求列表
        warehouses   : 仓库名称列表
        daily_cap    : 各仓库每日发货能力（车数上限）
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

    # ── 约束 2：仓库每日总发货 <= 产能上限 ──
    for w in warehouses:
        cap = daily_cap.get(w, 0)
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


