"""GSR-1.1.0 full pipeline integration test; synthetic data only."""
from __future__ import annotations
import importlib, json, traceback

MODULES=["strategy_registry","gsr_engine","gsr_data_store","gsr_data_adapter","gsr_historical_replay","gsr_validation_engine","gsr_regime_strategy_mapper"]

def main():
    r={"status":"PASS","modules":{},"checks":{}}
    m={}
    for name in MODULES:
        try:
            m[name]=importlib.import_module(name); r["modules"][name]="IMPORT_PASS"
        except Exception as e:
            r["status"]="FAIL"; r["modules"][name]=f"IMPORT_FAIL: {type(e).__name__}: {e}"; traceback.print_exc()
    if r["status"]=="FAIL": print(json.dumps(r,indent=2)); return 1
    reg=m["strategy_registry"]; core=m["gsr_engine"]; store=m["gsr_data_store"]; adapter=m["gsr_data_adapter"]; replay=m["gsr_historical_replay"]; val=m["gsr_validation_engine"]; mapper=m["gsr_regime_strategy_mapper"]
    n=len(reg.ATOMIC_STRATEGY_REGISTRY); r["checks"]["registry_113"]=(n==113)
    r["checks"]["core_smoke"]=core.smoke_test()
    store._self_test(); r["checks"]["raw_store"]="PASS"
    adapter._self_test(); r["checks"]["adapter"]="PASS"
    r["checks"]["replay"]=replay.run_self_test()
    r["checks"]["validation"]=val.run_self_test()
    r["checks"]["mapper"]=mapper.run_self_test()
    rows=mapper.map_history(mapper.synthetic_market_rows(),reg.ATOMIC_STRATEGY_REGISTRY)
    r["checks"]["registry_to_mapper"]=(len(rows)==3 and n==113)
    r["checks"]["no_live_order_permission"]=all(x.get("live_order_permission") is False for x in rows)
    ok=all([r["checks"]["registry_113"],r["checks"]["core_smoke"].get("ok") is True,r["checks"]["replay"].get("status")=="PASS",r["checks"]["validation"].get("status")=="PASS",r["checks"]["mapper"].get("passed") is True,r["checks"]["registry_to_mapper"],r["checks"]["no_live_order_permission"]])
    r["status"]="PASS" if ok else "FAIL"
    print(json.dumps(r,indent=2,ensure_ascii=False,default=str)); return 0 if ok else 1

if __name__=="__main__": raise SystemExit(main())
