"""Pickup-and-delivery vehicle routing with capacities and delivery deadlines (OR-Tools).

Each order is a pair: collect at the supplier, drop at the shelter, same vehicle, pickup
first. Vehicles start where they are (live GPS) or at their depot and end wherever their
last drop is. Travel times come from the risk-aware road router; a leg with no safe road
is given a prohibitive time so the solver avoids it. Orders that cannot be served on time
are dropped with a penalty weighted by the people they serve, never silently ignored:
the caller reports them.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ortools.constraint_solver import pywrapcp, routing_enums_pb2

UNREACHABLE_S = 10_000_000          # a leg with no safe road
HORIZON_S = 72 * 3600
PICKUP_SERVICE_S = 15 * 60
DROP_SERVICE_S = 10 * 60
SAME_PLACE_S = 3 * 60               # each extra load at a place the truck is already at
SPAN_COST = 5                       # per second of the longest trip: spreads loads so the last delivery lands sooner
LATE_COST_PER_S = 20                # a second late at a shelter costs as much as 20 s of driving


@dataclass
class Job:
    key: str                 # e.g. procurement id, or "id:chunk" when split
    pickup: int              # location index
    drop: int                # location index
    weight_kg: float
    deadline_s: int | None   # hard: seconds from now the drop must happen by (last safe road closes)
    value: float             # people served x urgency; sets the drop penalty
    soft_deadline_s: int | None = None   # wanted by: lateness is costed, not forbidden


@dataclass
class Vehicle:
    key: str
    start: int               # location index
    capacity_kg: float


@dataclass
class Stop:
    job: str
    kind: str                # pickup | drop
    location: int
    arrival_s: int


@dataclass
class Plan:
    routes: dict[str, list[Stop]] = field(default_factory=dict)
    dropped: list[str] = field(default_factory=list)
    status: str = "ok"


# Two ways to build a first plan; on real cases neither always wins (one tends to pile loads on
# one truck, the other to spread them), so both run and the cheaper result is kept.
STRATEGIES = ("PATH_CHEAPEST_ARC", "PARALLEL_CHEAPEST_INSERTION")


def solve(matrix_s: list[list[int]], vehicles: list[Vehicle], jobs: list[Job], time_limit_s: int = 5) -> Plan:
    if not vehicles or not jobs:
        return Plan(dropped=[j.key for j in jobs], status="nothing to plan" if not jobs else "no vehicles")
    best: tuple[int, Plan] | None = None
    per = max(1, time_limit_s // len(STRATEGIES))
    for strategy in STRATEGIES:
        got = _attempt(matrix_s, vehicles, jobs, strategy, per)
        if got is not None and (best is None or got[0] < best[0]):
            best = got
    if best is None:
        return Plan(dropped=[j.key for j in jobs], status="no feasible plan")
    return best[1]


def _attempt(matrix_s, vehicles: list[Vehicle], jobs: list[Job], strategy: str, seconds: int) -> tuple[int, Plan] | None:
    n_loc = len(matrix_s)
    # Node layout: one node per vehicle start, two per job, one shared open end.
    nodes_loc: list[int] = [v.start for v in vehicles]
    pickup_node, drop_node = {}, {}
    for j in jobs:
        pickup_node[j.key] = len(nodes_loc)
        nodes_loc.append(j.pickup)
        drop_node[j.key] = len(nodes_loc)
        nodes_loc.append(j.drop)
    end = len(nodes_loc)
    nodes_loc.append(-1)
    n = len(nodes_loc)

    service = [0] * n
    demand = [0] * n
    for j in jobs:
        service[pickup_node[j.key]] = PICKUP_SERVICE_S
        service[drop_node[j.key]] = DROP_SERVICE_S
        w = int(round(j.weight_kg))
        demand[pickup_node[j.key]] = w
        demand[drop_node[j.key]] = -w

    def travel(a: int, b: int) -> int:
        la, lb = nodes_loc[a], nodes_loc[b]
        if lb == -1 or la == -1:
            return 0
        assert 0 <= la < n_loc and 0 <= lb < n_loc
        return int(matrix_s[la][lb])

    mgr = pywrapcp.RoutingIndexManager(n, len(vehicles), [i for i in range(len(vehicles))], [end] * len(vehicles))
    routing = pywrapcp.RoutingModel(mgr)

    def time_cb(i, k):
        a, b = mgr.IndexToNode(i), mgr.IndexToNode(k)
        if service[a] and nodes_loc[a] == nodes_loc[b]:
            return SAME_PLACE_S     # loading several orders at one shop is one stop, not several
        return travel(a, b) + service[a]

    t_idx = routing.RegisterTransitCallback(time_cb)
    routing.SetArcCostEvaluatorOfAllVehicles(t_idx)
    routing.AddDimension(t_idx, HORIZON_S, UNREACHABLE_S + HORIZON_S, True, "time")
    tdim = routing.GetDimensionOrDie("time")
    tdim.SetGlobalSpanCostCoefficient(SPAN_COST)

    d_idx = routing.RegisterUnaryTransitCallback(lambda i: demand[mgr.IndexToNode(i)])
    routing.AddDimensionWithVehicleCapacity(d_idx, 0, [int(v.capacity_kg) for v in vehicles], True, "load")

    for j in jobs:
        p, d = mgr.NodeToIndex(pickup_node[j.key]), mgr.NodeToIndex(drop_node[j.key])
        routing.AddPickupAndDelivery(p, d)
        routing.solver().Add(routing.VehicleVar(p) == routing.VehicleVar(d))
        routing.solver().Add(tdim.CumulVar(p) <= tdim.CumulVar(d))
        if j.deadline_s is not None:
            tdim.CumulVar(d).SetMax(max(0, min(j.deadline_s, UNREACHABLE_S + HORIZON_S)))
        if j.soft_deadline_s is not None:
            tdim.SetCumulVarSoftUpperBound(d, max(0, j.soft_deadline_s), LATE_COST_PER_S)
        penalty = int(1_000_000 + 10_000 * max(j.value, 0))
        routing.AddDisjunction([p], penalty)
        routing.AddDisjunction([d], penalty)
    # Legs that are unreachable must never be used: forbid arcs with no safe road.
    for a in range(n):
        for b in range(n):
            if a != b and travel(a, b) >= UNREACHABLE_S:
                ia, ib = mgr.NodeToIndex(a), mgr.NodeToIndex(b)
                if ia >= 0 and ib >= 0 and routing.NextVar(ia) is not None:
                    routing.NextVar(ia).RemoveValue(ib)

    params = pywrapcp.DefaultRoutingSearchParameters()
    params.first_solution_strategy = getattr(routing_enums_pb2.FirstSolutionStrategy, strategy)
    params.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    params.time_limit.seconds = seconds
    sol = routing.SolveWithParameters(params)
    if sol is None:
        return None

    plan = Plan(status=f"ok ({strategy.lower().replace('_', ' ')})")
    node_job = {v: (k, "pickup") for k, v in pickup_node.items()} | {v: (k, "drop") for k, v in drop_node.items()}
    served = set()
    for vi, v in enumerate(vehicles):
        idx = routing.Start(vi)
        stops = []
        while not routing.IsEnd(idx):
            node = mgr.IndexToNode(idx)
            if node in node_job:
                key, kind = node_job[node]
                stops.append(Stop(key, kind, nodes_loc[node], sol.Value(tdim.CumulVar(idx))))
                served.add(key)
            idx = sol.Value(routing.NextVar(idx))
        if stops:
            plan.routes[v.key] = stops
    plan.dropped = [j.key for j in jobs if j.key not in served]
    return sol.ObjectiveValue(), plan


def split_by_capacity(quantity: float, kg_per_unit: float, max_capacity_kg: float) -> list[float]:
    """Split an order into chunks no heavier than the largest vehicle."""
    if kg_per_unit <= 0:
        return [quantity]
    per_chunk = max(1.0, max_capacity_kg / kg_per_unit)
    chunks = []
    left = quantity
    while left > 1e-9:
        c = min(left, per_chunk)
        chunks.append(c)
        left -= c
    return chunks
