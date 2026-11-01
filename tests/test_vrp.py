"""Vehicle routing on small hand-built matrices (test inputs only)."""

from functools import partial

from rasad.engine import vrp
from rasad.engine.vrp import UNREACHABLE_S, Job, Vehicle, split_by_capacity

solve = partial(vrp.solve, time_limit_s=2)

# Locations: 0 depot, 1 supplier A, 2 supplier B, 3 shelter X, 4 shelter Y. Minutes x 60.
M = [[0, 10, 30, 40, 50],
     [10, 0, 25, 20, 45],
     [30, 25, 0, 35, 15],
     [40, 20, 35, 0, 30],
     [50, 45, 15, 30, 0]]
M = [[m * 60 for m in row] for row in M]


def test_pickup_before_drop_same_vehicle():
    plan = solve(M, [Vehicle("v1", 0, 2000)], [Job("o1", 1, 3, 500, None, 100), Job("o2", 2, 4, 500, None, 100)])
    assert plan.dropped == []
    stops = plan.routes["v1"]
    order = [(s.job, s.kind) for s in stops]
    for job in ("o1", "o2"):
        assert order.index((job, "pickup")) < order.index((job, "drop"))
    assert all(a.arrival_s <= b.arrival_s for a, b in zip(stops, stops[1:]))


def test_capacity_forces_two_vehicles():
    plan = solve(M, [Vehicle("v1", 0, 1000), Vehicle("v2", 0, 1000)],
                 [Job("o1", 1, 3, 900, None, 100), Job("o2", 1, 3, 900, None, 100)])
    assert plan.dropped == [] and len(plan.routes) == 2


def test_deadline_drops_late_order_and_keeps_urgent_one():
    # Only one vehicle; o2 cannot reach shelter Y within 20 minutes from the depot.
    plan = solve(M, [Vehicle("v1", 0, 2000)], [Job("o1", 1, 3, 100, 3 * 3600, 200), Job("o2", 2, 4, 100, 20 * 60, 50)])
    assert "o2" in plan.dropped and "o1" not in plan.dropped


def test_unreachable_leg_is_never_used():
    m = [row[:] for row in M]
    for a in range(5):
        m[a][3] = UNREACHABLE_S if a != 3 else 0   # shelter X has no safe road in
    plan = solve(m, [Vehicle("v1", 0, 2000)], [Job("o1", 1, 3, 100, None, 100), Job("o2", 2, 4, 100, None, 100)])
    assert plan.dropped == ["o1"]
    assert [s.job for s in plan.routes["v1"]] == ["o2", "o2"]


def test_no_vehicles_or_jobs():
    assert solve(M, [], [Job("o1", 1, 3, 1, None, 1)]).dropped == ["o1"]
    assert solve(M, [Vehicle("v", 0, 1)], []).status == "nothing to plan"


def test_split_by_capacity():
    assert split_by_capacity(20000, 1.0, 7500) == [7500, 7500, 5000]
    assert split_by_capacity(5000, 0.001, 1000) == [5000]          # soap in grams fits easily
    assert sum(split_by_capacity(1234.5, 1.0, 500)) == 1234.5


def test_soft_deadline_orders_urgent_drop_first_but_keeps_both():
    # Two drops reachable in either order; the one wanted sooner goes first, nothing is dropped.
    jobs = [Job("late", 1, 4, 100, None, 100, soft_deadline_s=10 * 3600),
            Job("soon", 1, 3, 100, None, 100, soft_deadline_s=40 * 60)]
    plan = solve(M, [Vehicle("v1", 0, 2000)], jobs)
    assert plan.dropped == []
    drops = [s.job for s in plan.routes["v1"] if s.kind == "drop"]
    assert drops == ["soon", "late"]


def test_soft_deadline_missed_is_still_served():
    plan = solve(M, [Vehicle("v1", 0, 2000)], [Job("o1", 2, 4, 100, None, 100, soft_deadline_s=60)])
    assert plan.dropped == [] and plan.routes["v1"][-1].arrival_s > 60


def test_work_is_spread_across_drivers():
    # One truck could carry both loads (less total driving), but two trucks get the last one there sooner.
    jobs = [Job("x", 1, 3, 100, None, 100), Job("y", 1, 4, 100, None, 100)]
    plan = solve(M, [Vehicle("v1", 0, 2000), Vehicle("v2", 0, 2000)], jobs)
    assert plan.dropped == [] and len(plan.routes) == 2


def test_loading_several_orders_at_one_place_is_one_stop():
    jobs = [Job("a", 1, 3, 100, None, 100), Job("b", 1, 3, 100, None, 100)]
    plan = solve(M, [Vehicle("v1", 0, 2000)], jobs)
    stops = plan.routes["v1"]
    picks = [s for s in stops if s.kind == "pickup"]
    assert picks[1].arrival_s - picks[0].arrival_s == vrp.SAME_PLACE_S
