#!/usr/bin/env python3
"""Guard: the traffic generator must write in batches.

It used to issue a round trip per order, another per line item and one more
for the total, all sequentially. A run asked for 100 orders a second and
delivered 29, and nothing said so -- the tick counter still printed +100 and
the only clue was that ticks were 3.4 seconds apart. Every lag number taken
that way measured psycopg2's round trips rather than the pipeline (#182).

That is the failure worth guarding: it does not error, it just quietly makes
the load smaller than the number you typed, which flatters every result taken
under it. A later simplification back to a loop would be entirely reasonable
looking and would restore the problem in full.

Runs offline.
"""

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "scripts" / "simulate_live_traffic.py"
failures = []


def check(label, condition, detail=""):
    if condition:
        print(f"  ok    {label}")
    else:
        print(f"  FAIL  {label}{(' — ' + detail) if detail else ''}")
        failures.append(label)


tree = ast.parse(SRC.read_text(encoding="utf-8"))
funcs = {n.name: n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}

for name in ("place_orders", "advance_statuses"):
    check(f"{name} exists", name in funcs)

for name in ("place_orders", "advance_statuses"):
    if name not in funcs:
        continue
    fn = funcs[name]

    calls = [
        n.func.attr for n in ast.walk(fn)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
    ]
    names = [
        n.func.id for n in ast.walk(fn)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
    ]
    check(
        f"{name} batches its writes",
        "execute_values" in names + calls,
        "without it the write cost scales with the rate and the generator "
        "silently becomes the bottleneck",
    )

    # The specific shape that caused it: a database call inside a loop. One
    # statement per row is the thing being ruled out, not loops in general --
    # building the rows in Python first is exactly what the fix does.
    in_loop = []
    for loop in [n for n in ast.walk(fn) if isinstance(n, (ast.For, ast.While))]:
        for node in ast.walk(loop):
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and node.func.attr in ("execute", "executemany")
                    and isinstance(node.func.value, ast.Name)
                    and node.func.value.id == "cur"):
                in_loop.append(node.lineno)
    check(
        f"{name} issues no per-row statement",
        not in_loop,
        f"cur.execute inside a loop at line(s) {in_loop}",
    )

# The totals update has to survive batching. The gold layer reconciles order
# totals against the sum of their line items, so an order whose total was
# never written back is a mart that silently disagrees with its source.
place = SRC.read_text(encoding="utf-8")[
    SRC.read_text(encoding="utf-8").index("def place_orders"):
]
place = place[: place.index("\ndef ")]
check(
    "order totals are still written back",
    "UPDATE orders SET total_amount" in place,
    "gold reconciles totals against the line items",
)
check(
    "the update still bumps updated_at",
    "updated_at = CURRENT_TIMESTAMP" in place,
    "without it the row changes but emits no CDC update the mirror can see",
)

print()
if failures:
    print(f"{len(failures)} check(s) failed")
    sys.exit(1)
print("All traffic generator guards passed")
