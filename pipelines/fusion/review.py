"""Prepare decision-blind evidence and score two independent human reviews.

Labels describe support in the supplied measurements, not verified physical
events. Reviewers receive review.html only; the project lead keeps review-key.json.
"""

# The self-contained HTML/JavaScript below has long template lines.
# ruff: noqa: E501

from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
from datetime import date, timedelta

import pandas as pd

from pipelines import storage

ALERT_KEY = ["locationid", "date_local", "parameter"]
EVENT_LABELS = {"supported", "not_supported", "uncertain"}
QUALITY_LABELS = {"issue", "no_issue", "uncertain"}


def _records(frame: pd.DataFrame) -> list[dict]:
    return json.loads(frame.to_json(orient="records", date_format="iso"))


def _series(frame: pd.DataFrame) -> list[dict]:
    if frame.empty:
        return []
    readings = frame.sort_values(["sensor_id", "datetime"]).copy()
    local = pd.to_datetime(readings["datetime_local"], errors="coerce")
    readings["hour"] = local.dt.hour + local.dt.minute / 60
    readings["datetime_utc"] = readings["datetime"]
    return _records(readings[[
        "sensor_id", "datetime_utc", "hour", "value", "unit", "original_unit",
    ]])


def _distance(a: tuple[float, float], b: tuple[float, float]) -> float:
    lat1, lon1, lat2, lon2 = map(math.radians, (*a, *b))
    h = math.sin((lat2 - lat1) / 2) ** 2
    h += math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return 6371 * 2 * math.asin(math.sqrt(min(1.0, h)))


def _evidence(silver: pd.DataFrame, location: int, day: str, parameter: str) -> dict:
    same_parameter = silver[silver["parameter"] == parameter]
    station = same_parameter[same_parameter["locationid"] == location]
    current = station[station["date_local"] == day]
    start = (date.fromisoformat(day) - timedelta(days=7)).isoformat()
    history = station[(station["date_local"] >= start) & (station["date_local"] < day)].copy()
    history["hour"] = pd.to_datetime(history["datetime_local"], errors="coerce").dt.hour
    profile = history.groupby(["unit", "hour"], dropna=False).agg(
        median=("value", "median"), q25=("value", lambda x: x.quantile(.25)),
        q75=("value", lambda x: x.quantile(.75)), readings=("value", "count"),
        observed_days=("date_local", "nunique"),
    ).reset_index()
    peers = []
    coordinates = station[station["date_local"] <= day][["latitude", "longitude"]].dropna()
    if not coordinates.empty:
        origin = tuple(coordinates.iloc[-1].astype(float))
        others = same_parameter[
            (same_parameter["date_local"] == day) & (same_parameter["locationid"] != location)
        ]
        candidates = []
        for peer, group in others.groupby("locationid"):
            coords = group[["latitude", "longitude"]].dropna()
            if not coords.empty:
                distance = _distance(origin, tuple(coords.iloc[-1].astype(float)))
                if distance <= 50:
                    candidates.append((distance, int(peer), group))
        for distance, peer, group in sorted(candidates, key=lambda x: (x[0], x[1]))[:3]:
            peers.append({"locationid": peer, "distance_km": round(distance, 2),
                          "readings": _series(group)})
    return {
        "locationid": location, "date_local": day, "parameter": parameter,
        "current_readings": _series(current), "prior_seven_day_hourly_profile": _records(profile),
        "history_reading_rows": len(history),
        "history_observed_days": int(history["date_local"].nunique()),
        "nearby_stations": peers,
    }


def prepare_review(
    fused: pd.DataFrame, silver: pd.DataFrame, *, per_status: int = 10, seed: int = 42,
) -> tuple[dict, dict]:
    """Sample PM2.5 alerts per status; never substitute another parameter or stratum."""
    if per_status < 1:
        raise ValueError("per_status must be positive")
    if fused.duplicated(ALERT_KEY).any():
        raise ValueError("Review requires unique station-day-parameter alert keys")
    eligible = fused[fused["parameter"] == "pm25"].sort_values(ALERT_KEY)
    rng = random.Random(seed)
    selected, strata = [], []
    for status in ("escalated", "quarantined"):
        group = eligible[eligible["status"] == status]
        count = min(per_status, len(group))
        selected.extend(group.iloc[rng.sample(range(len(group)), count)].to_dict("records"))
        strata.append({"status": status, "population": len(group), "requested": per_status,
                       "selected": count, "shortfall": per_status - count})
    rng.shuffle(selected)
    evidence, answer_key = [], []
    for row in selected:
        location, day = int(row["locationid"]), str(row["date_local"])
        identity = json.dumps([location, day, "pm25"], separators=(",", ":"))
        case_id = "case-" + hashlib.sha256(identity.encode()).hexdigest()[:12]
        evidence.append({"case_id": case_id, **_evidence(silver, location, day, "pm25")})
        answer_key.append({"case_id": case_id, "locationid": location, "date_local": day,
                           "parameter": "pm25", "status": row["status"],
                           "incident_rule_ids": row["incident_rule_ids"]})
    packed = json.dumps(evidence, sort_keys=True, allow_nan=False)
    pack_id = hashlib.sha256(packed.encode()).hexdigest()
    packet = {"pack_id": pack_id, "cases": evidence}
    key = {"pack_id": pack_id, "seed": seed, "parameter": "pm25", "strata": strata,
           "cases": answer_key, "reviewers_required": 2,
           "sampling_warning": "Balanced status sample; pooled metrics are not population estimates"}
    return packet, key


def _load_review(review: dict, key: dict) -> tuple[str, dict]:
    if review.get("pack_id") != key["pack_id"]:
        raise ValueError("Review belongs to a different evidence pack")
    reviewer = review.get("reviewer_id")
    if not isinstance(reviewer, str) or not reviewer.strip():
        raise ValueError("A named reviewer_id is required")
    reviewer = reviewer.strip()
    known = {item["case_id"] for item in key["cases"]}
    responses, seen = {}, set()
    for row in review.get("responses", []):
        case_id = row.get("case_id")
        if case_id not in known or case_id in seen:
            raise ValueError("Unknown or duplicate review case_id")
        seen.add(case_id)
        event, quality = row.get("pollution_evidence"), row.get("data_quality")
        if event not in EVENT_LABELS | {None, ""} or quality not in QUALITY_LABELS | {None, ""}:
            raise ValueError("Unsupported review label")
        notes = row.get("notes")
        if event and quality and (not isinstance(notes, str) or not notes.strip()):
            raise ValueError("Completed judgments require a short evidence note")
        if event and quality:
            responses[case_id] = row
    return reviewer, responses


def summarize_reviews(key: dict, review_a: dict | None = None, review_b: dict | None = None) -> dict:
    """Score paired complete judgments, preserving uncertainty and sample denominators."""
    parsed = [_load_review(review, key) for review in (review_a, review_b) if review is not None]
    names = [name for name, _ in parsed]
    normalized = [" ".join(name.casefold().split()) for name in names]
    if len(normalized) != len(set(normalized)):
        raise ValueError("Two distinct reviewers are required")
    responses = [items for _, items in parsed]
    paired = set.intersection(*(set(items) for items in responses)) if len(responses) == 2 else set()
    summaries, disagreements, uncertain = [], [], []
    for case in key["cases"]:
        identity = case["case_id"]
        if identity not in paired:
            continue
        a, b = (items[identity] for items in responses)
        same_event = a["pollution_evidence"] == b["pollution_evidence"]
        same_quality = a["data_quality"] == b["data_quality"]
        if not same_event or not same_quality:
            disagreements.append(identity)
        if any(row[field] == "uncertain" for row in (a, b)
               for field in ("pollution_evidence", "data_quality")):
            uncertain.append(identity)
        summaries.append({**case, "event_agreement": same_event, "quality_agreement": same_quality,
                          "event": a["pollution_evidence"] if same_event else None,
                          "quality": a["data_quality"] if same_quality else None})

    def counts(rows):
        events = [row for row in rows if row["event"] == "supported"]
        non_events = [row for row in rows if row["event"] == "not_supported"]
        issues = [row for row in non_events if row["quality"] == "issue"]
        def held(group):
            return sum(row["status"] == "quarantined" for row in group)
        return {
            "paired_cases": len(rows), "consensus_supported_events": len(events),
            "supported_events_escalated": len(events) - held(events),
            "supported_events_quarantined": held(events),
            "supported_event_escalation_rate": (len(events) - held(events)) / len(events)
            if events else None,
            "consensus_non_events": len(non_events), "non_events_quarantined": held(non_events),
            "non_event_quarantine_rate": held(non_events) / len(non_events) if non_events else None,
            "consensus_data_issue_non_events": len(issues),
            "data_issue_non_events_quarantined": held(issues),
        }

    return {
        "status": "pending" if len(paired) < len(key["cases"]) or not key["cases"] else "reviewed",
        "expected_cases": len(key["cases"]), "reviewers": names,
        "complete_judgments_by_reviewer": {name: len(items) for name, items in parsed},
        "paired_complete_cases": len(paired), "unpaired_cases": len(key["cases"]) - len(paired),
        "event_agreement": sum(row["event_agreement"] for row in summaries) / len(paired)
        if paired else None,
        "quality_agreement": sum(row["quality_agreement"] for row in summaries) / len(paired)
        if paired else None,
        "disagreement_case_ids": disagreements, "uncertain_case_ids": uncertain,
        "sample_only": counts(summaries),
        "by_status": {status: counts([row for row in summaries if row["status"] == status])
                      for status in ("escalated", "quarantined")},
        "limitations": [
            "Human judgments are evidence support, not reference-monitor ground truth.",
            "Balanced status sampling prevents treating pooled rates as population accuracy.",
            "Disagreement and uncertainty remain unresolved; no automatic adjudication.",
        ],
    }


def review_html(packet: dict) -> str:
    """Self-contained offline page; no decisions, weak labels, external scripts or persistence."""
    data = json.dumps(packet, allow_nan=False).replace("<", "\\u003c")
    return _HTML.replace("__PACKET__", data)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("key", "review-a", "review-b", "output"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args(argv)
    try:
        paths = [args.key, args.review_a, args.review_b, args.output]
        if any(storage.is_s3(path) for path in paths):
            raise ValueError("Review files must be local")
        if storage.exists(args.output):
            raise ValueError("Choose a new output filename to preserve prior results")
        key, a, b = (json.loads(storage.read_text(path)) for path in paths[:3])
        report = summarize_reviews(key, a, b)
        storage.write_text(args.output, json.dumps(report, indent=2) + "\n")
    except (ValueError, TypeError) as exc:
        parser.error(str(exc))
    print(json.dumps(report["sample_only"], indent=2))
    return 0


_HTML = r'''<!doctype html>
<html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<title>DataGuard independent evidence review</title>
<style>
body{font:16px system-ui;background:#f3f6fa;color:#172b42;margin:0}main{max-width:1060px;margin:auto;padding:28px}
section,header{background:white;border:1px solid #d7e1ec;border-radius:12px;padding:22px;margin-bottom:18px}
h1{margin-top:0}p{line-height:1.5}.muted{color:#52687d}button,select,input,textarea{font:inherit;padding:10px;border:1px solid #a6b8ca;border-radius:6px}
button{cursor:pointer;background:#184e77;color:white}button:disabled{opacity:.4;cursor:default}nav{display:flex;gap:12px;align-items:center;flex-wrap:wrap}
label{display:block;margin:14px 0}select,textarea{display:block;width:100%;box-sizing:border-box;margin-top:6px}textarea{height:85px}
svg{width:100%;height:auto;background:#fafcfe}.legend{font-size:14px;line-height:1.6}pre{white-space:pre-wrap;font-size:12px;max-height:350px;overflow:auto}
.warn{background:#fff4d9;padding:12px;border-radius:6px}#message{color:#8b3408}a{color:#184e77}
</style><main>
<header><h1>Independent evidence review</h1>
<p>Review the measurements before discussing them with your teammate. Judge whether the day's pattern supports a pollution event and whether the measurements show a data issue. An event and a data issue can coexist. Select <b>uncertain</b> when the evidence is insufficient.</p>
<p class="muted">These are alerts selected for review, not confirmed events. The prior seven days and nearby stations provide context, not ground truth. Different units are plotted separately. No health threshold is implied. Decisions and model labels are withheld from this page.</p>
<p class="warn">Both reviewers open their own fresh copy of this page. Answers stay in memory only: download your JSON before closing or refreshing. Do not view your teammate's answers before submitting.</p>
<label>Your name or unique reviewer ID <input id="reviewer" autocomplete="off"></label>
<nav><button id="download">Download review JSON</button><span id="progress"></span></nav><p id="message" role="status"></p></header>
<section><nav><button id="previous">Previous</button><span id="position"></span><button id="next">Next</button></nav>
<h2 id="title"></h2><p id="coverage" class="muted"></p><div id="charts"></div>
<label>Pollution-event evidence<select id="event"><option value="">Choose a judgment</option><option value="supported">Supported by the supplied pattern</option><option value="not_supported">Not supported by the supplied pattern</option><option value="uncertain">Uncertain</option></select></label>
<label>Data quality<select id="quality"><option value="">Choose a judgment</option><option value="issue">Evidence of a data issue</option><option value="no_issue">No clear issue in supplied evidence</option><option value="uncertain">Uncertain</option></select></label>
<label>Short evidence note (required for a completed judgment)<textarea id="notes" placeholder="Mention the pattern, units, gaps, frozen values or peer comparison you used."></textarea></label>
<details><summary>Inspect exact readings and baseline coverage</summary><pre id="raw"></pre></details></section></main>
<script id="packet" type="application/json">__PACKET__</script>
<script>
const packet=JSON.parse(document.getElementById('packet').textContent), cases=packet.cases;
const answers=cases.map(c=>({case_id:c.case_id,pollution_evidence:'',data_quality:'',notes:''}));let index=0;
const el=id=>document.getElementById(id), colors=['#176a9c','#ba3658','#328455','#9963b8','#c16a10','#497c82'];
function progress(){el('progress').textContent=answers.filter(a=>a.pollution_evidence&&a.data_quality&&a.notes.trim()).length+' / '+cases.length+' completed';}
function chart(c){
 const traces=[];const groups=(rows,prefix)=>{const m=new Map();for(const r of rows){const key=prefix+' sensor '+r.sensor_id+' | '+r.unit;if(!m.has(key))m.set(key,{name:key,unit:r.unit,points:[]});if(Number.isFinite(r.value)&&Number.isFinite(r.hour))m.get(key).points.push([r.hour,r.value]);}return [...m.values()];};
 traces.push(...groups(c.current_readings,'Target'));
 for(const peer of c.nearby_stations)traces.push(...groups(peer.readings,'Peer '+peer.locationid));
 const units=[...new Set([...traces.map(t=>t.unit),...c.prior_seven_day_hourly_profile.map(r=>r.unit)])];
 el('charts').replaceChildren();
 for(const unit of units){
  const selected=traces.filter(t=>t.unit===unit), profile=c.prior_seven_day_hourly_profile.filter(r=>r.unit===unit&&Number.isFinite(r.hour)&&Number.isFinite(r.median));
  const values=selected.flatMap(t=>t.points.map(p=>p[1])).concat(profile.flatMap(p=>[p.q25,p.q75]).filter(Number.isFinite));
  if(!values.length)continue;
  const low=Math.min(0,...values),high=Math.max(1,...values),span=high-low||1,x=h=>55+h/24*835,y=v=>245-(v-low)/span*210;
  const ns='http://www.w3.org/2000/svg',svg=document.createElementNS(ns,'svg');svg.setAttribute('viewBox','0 0 950 290');svg.setAttribute('role','img');svg.setAttribute('aria-label','Observed concentrations in '+unit+' by local hour');
  function node(tag,attrs,text){const n=document.createElementNS(ns,tag);for(const [k,v]of Object.entries(attrs))n.setAttribute(k,v);if(text!==undefined)n.textContent=text;svg.append(n);return n;}
  for(const h of [0,6,12,18,24]){node('line',{x1:x(h),x2:x(h),y1:30,y2:245,stroke:'#e0e7ee'});node('text',{x:x(h),y:270,'text-anchor':'middle','font-size':13},h+'h');}
  for(const v of [low,(low+high)/2,high])node('text',{x:48,y:y(v)+5,'text-anchor':'end','font-size':12},v.toFixed(1));
  if(profile.length){const upper=profile.map(p=>[x(p.hour),y(p.q75)]),lower=[...profile].reverse().map(p=>[x(p.hour),y(p.q25)]);node('polygon',{points:upper.concat(lower).map(p=>p.join(',')).join(' '),fill:'#dce4ed',opacity:.7});node('polyline',{points:profile.map(p=>x(p.hour)+','+y(p.median)).join(' '),fill:'none',stroke:'#64748b','stroke-dasharray':'5 4','stroke-width':2});}
  const legend=document.createElement('p');legend.className='legend';legend.textContent=unit+' · gray: prior 7-day median and middle 50%';
  selected.forEach((t,i)=>{const color=colors[i%colors.length];node('polyline',{points:t.points.map(p=>x(p[0])+','+y(p[1])).join(' '),fill:'none',stroke:color,'stroke-width':t.name.startsWith('Target')?2.5:1.5,opacity:t.name.startsWith('Target')?1:.65});for(const point of t.points)node('circle',{cx:x(point[0]),cy:y(point[1]),r:2,fill:color});const span=document.createElement('span');span.textContent=' · '+t.name;span.style.color=color;legend.append(span);});
  el('charts').append(svg,legend);
 }
 if(!units.length)el('charts').textContent='No readings available for this case.';
}
function render(){progress();el('previous').disabled=index===0;el('next').disabled=index>=cases.length-1;el('position').textContent=cases.length?'Case '+(index+1)+' of '+cases.length:'No eligible cases';if(!cases.length)return;const c=cases[index],a=answers[index];el('title').textContent=c.case_id+' · station '+c.locationid+' · '+c.date_local+' · '+c.parameter;el('coverage').textContent=c.current_readings.length+' current readings; '+c.history_observed_days+' prior observed days ('+c.history_reading_rows+' readings); '+c.nearby_stations.length+' peers within 50 km. Lines connect observations; inspect timestamps for gaps.';chart(c);el('raw').textContent=JSON.stringify(c,null,2);el('event').value=a.pollution_evidence;el('quality').value=a.data_quality;el('notes').value=a.notes;}
function save(){if(!cases.length)return;answers[index].pollution_evidence=el('event').value;answers[index].data_quality=el('quality').value;answers[index].notes=el('notes').value;progress();}
for(const id of ['event','quality','notes'])el(id).addEventListener('input',save);
el('previous').onclick=()=>{save();if(index>0)index--;render();};el('next').onclick=()=>{save();if(index<cases.length-1)index++;render();};
el('download').onclick=()=>{save();const reviewer=el('reviewer').value.trim();if(!reviewer){el('message').textContent='Enter your reviewer ID first.';return;}if(answers.some(a=>a.pollution_evidence&&a.data_quality&&!a.notes.trim())){el('message').textContent='Add an evidence note to each completed judgment.';return;}const report={pack_id:packet.pack_id,reviewer_id:reviewer,responses:answers};const url=URL.createObjectURL(new Blob([JSON.stringify(report,null,2)],{type:'application/json'}));const link=document.createElement('a');link.href=url;link.download='review-'+reviewer.replace(/[^a-z0-9_-]/gi,'_')+'.json';link.click();setTimeout(()=>URL.revokeObjectURL(url),1000);el('message').textContent='Downloaded. Keep your answers private until both reviewers finish.';};
render();
</script></html>'''


if __name__ == "__main__":
    raise SystemExit(main())
