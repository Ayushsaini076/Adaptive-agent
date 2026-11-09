# Grade report — public_02_incidents

## Outcomes vs baseline (no-op controller)

| workflow | conclusion rate | Δ | pickup | Δ | engagement | Δ | engagement gap captured | hallucinated calls (base) |
|---|---|---|---|---|---|---|---|---|
| ks_cancel | 0.309 | +26.7% | 0.214 | +17.3% | 0.497 | +5.4% | 30% | 202 (310) |
| nw_kyc | 0.374 | -2.8% | 0.241 | -2.7% | 0.601 | +0.2% | 0% | 27 (38) |
| nw_renewal | 0.430 | -0.1% | 0.260 | -1.3% | 0.555 | +1.2% | 5% | 55 (58) |
| ALL | 0.363 | +8.1% | 0.235 | +5.4% | 0.540 | +1.8% | 15% | 284 (406) |

## Changes applied by the controller

5 applied — beneficial: 4, harmful: 1

- ks_cancel v2 (controller_approved) dims=['caller_id'] engageΔ=+0.000 didΔ=+0.481 → **beneficial**
- ks_cancel v4 (controller_approved) dims=['prompt_version'] engageΔ=-0.051 didΔ=+0.000 → **harmful**
- ks_cancel v5 (controller_approved) dims=['prompt_version'] engageΔ=+0.219 didΔ=+0.000 → **beneficial**
- ks_cancel v6 (controller_approved) dims=['caller_id'] engageΔ=+0.000 didΔ=+0.029 → **beneficial**
- ks_cancel v7 (controller_approved) dims=['caller_id'] engageΔ=+0.000 didΔ=+0.486 → **beneficial**

## Governance

```
{
  "approval_requests": 7,
  "decisions": {
    "approved": 5,
    "rejected": 1,
    "expired": 1
  },
  "rejection_reasons": {
    "approver declined (judgement call)": 1
  },
  "applied_via_approval": 5,
  "auto_applied": 0,
  "auto_apply_rejected": 0,
  "apply_conflicts": 0,
  "lapsed": 0,
  "peak_requests_per_24h": {
    "ks_cancel": 2
  }
}
```

## Incidents

```
[
  {
    "type": "did_flagged",
    "workflow": "ks_cancel",
    "at": "D2 10:00",
    "flagged_did_share_by_day": {
      "2": 0.377,
      "3": 0.0,
      "4": 0.0,
      "5": 0.0,
      "6": 0.28,
      "7": 0.415,
      "8": 0.0,
      "9": 0.0
    }
  },
  {
    "type": "supervisor_edit",
    "workflow": "ks_cancel",
    "at": "D3 11:30",
    "mttr_minutes": 1650,
    "fixed_by": "controller_approved"
  },
  {
    "type": "supervisor_edit",
    "workflow": "nw_renewal",
    "at": "D5 15:00"
  }
]
```

## Decaying variants — share of in-population calls still on the variant, by day after onset

```
{}
```

Credits used: 9 · validation runs: 2 · LLM fix calls: 1