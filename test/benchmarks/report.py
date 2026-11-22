#!/usr/bin/env python3
"""Turns the JSON files the benchmarks write (BENCH_OUT) into the markdown tables used in BENCHMARKS.md.

usage: report.py <dir with the json files> [section]
"""
import json
import os
import sys

D = sys.argv[1]
ONLY = sys.argv[2] if len(sys.argv) > 2 else ""


def load(name):
    with open(os.path.join(D, name + ".json")) as f:
        return json.load(f)


def num(x, d=1):
    if abs(x) >= 1000:
        return f"{x:,.0f}"
    return f"{x:.{d}f}"


def sp(s, d=1):
    """median (min-max) of a number measured once per repetition"""
    if s["min"] == s["max"]:
        return num(s["median"], d)
    return f"{num(s['median'], d)} ({num(s['min'], d)}-{num(s['max'], d)})"


def dist(x, d=1):
    return [num(x["p50"], d), num(x["p95"], d), num(x["p99"], d), num(x["max"], d)]


def ratio(r):
    return f"{r['value']:.2f}x (95% CI {r['ci95_lo']:.2f}-{r['ci95_hi']:.2f})"


def table(head, rows):
    out = ["| " + " | ".join(head) + " |", "|" + "---|" * len(head)]
    for r in rows:
        out.append("| " + " | ".join(str(c) for c in r) + " |")
    return "\n".join(out)


def env():
    e = load("dedup-docker_layers")["env"]
    print(f"Machine: {e['cpu']} ({e['threads']} threads), kernel {e['kernel']}, {e['go']}, commit {e['commit']}, run on {e['date']}; {e['warmup_reps']} warm-up and {e['measured_reps']} measured repetitions per benchmark.")


def dedup():
    for name, title in [("docker_layers", "A. Docker layers"), ("python_versions", "C. Python 3.11 / 3.10 / 3.9"), ("shared_base", "D. Three apps on one base image"), ("spring_base", "E. Three Spring Boot services on one JRE base"), ("raw_media", "B. Random media")]:
        if not os.path.exists(os.path.join(D, "dedup-" + name + ".json")):
            continue
        r = load("dedup-" + name)["results"]
        on, off = r["dedup_on"], r["dedup_off"]
        print(f"\n**{title}** ({r['files']} files, {r['uploaded_mib']:.0f} MiB, {r['chunks']} chunks)\n")
        rows = [
            ["Chunks deduplicated", sp(on["chunks_deduplicated"], 0), sp(off["chunks_deduplicated"], 0)],
            ["Bytes sent to workers (MiB)", sp(on["bytes_sent_to_workers_mib"], 0), sp(off["bytes_sent_to_workers_mib"], 0)],
            ["Bytes on worker disks (MiB)", sp(on["bytes_on_worker_disks_mib"], 0), sp(off["bytes_on_worker_disks_mib"], 0)],
            ["Storage ratio", sp(on["storage_ratio"], 2), "n/a"],
            ["Bloom false positives (% of checks)", sp(on["bloom_false_positive_pct"], 2), "n/a"],
        ]
        for lab, key in [("P50", 0), ("P95", 1), ("P99", 2), ("max", 3)]:
            rows.append([f"Upload ms per chunk, {lab} (n={on['upload_ms_per_chunk']['pooled']['n']} files)", dist(on["upload_ms_per_chunk"]["pooled"])[key], dist(off["upload_ms_per_chunk"]["pooled"])[key]])
        rows.append(["Whole upload, seconds", sp(on["upload_seconds"]), sp(off["upload_seconds"])])
        rows.append(["Throughput, MiB/s", sp(on["throughput_mib_per_s"]), sp(off["throughput_mib_per_s"])])
        print(table(["Metric (median over reps, min-max)", "Dedup on", "Dedup off"], rows))
        print(f"\nUpload time off / on: {ratio(r['upload_time_off_over_on'])}")


def avail():
    rows = []
    for name, title in [("rf2_one_down", "RF=2, 1 worker down"), ("rf1_one_down", "RF=1, 1 worker down"), ("rf2_two_down", "RF=2, 2 workers down"), ("rf1_two_down", "RF=1, 2 workers down")]:
        r = load("availability-" + name)["results"]
        rows.append([title, r["objects_per_rep"], sp(r["served_correctly"], 0), sp(r["unavailable"], 0), f"{r['unavailable_pct_all_reps']:.1f}%", sp(r["served_corrupt"], 0), sp(r["expected_unavailable"], 0)])
    print(table(["Configuration", "Objects per rep", "Served correctly", "Unavailable", "Unavailable, all reps pooled", "Served corrupt", "Expected unavailable"], rows))
    print()
    rows = []
    for name, title in [("rf2_one_down", "RF=2, 1 down"), ("rf1_one_down", "RF=1, 1 down"), ("rf2_two_down", "RF=2, 2 down"), ("rf1_two_down", "RF=1, 2 down")]:
        r = load("availability-" + name)["results"]
        b, a = r["read_ms_before_failure"], r["read_ms_after_failure"]
        rows.append([title, "before"] + dist(b["pooled"], 2) + [b["pooled"]["n"]])
        rows.append([title, "after"] + dist(a["pooled"], 2) + [a["pooled"]["n"]])
    print(table(["Configuration", "Reads", "P50 ms", "P95 ms", "P99 ms", "max ms", "n"], rows))
    r = load("availability-rf2_two_down")["results"]
    print(f"\nStatus codes of the unavailable objects, RF=2 / 2 down, all reps: {r['unavailable_status_codes_all_reps']}")
    d = load("availability-detection")["results"]["seconds"]
    print(f"\nFailure detection (seconds from kill to OFFLINE, n={d['n']} kills): min {d['min']:.2f}, P50 {d['p50']:.2f}, P95 {d['p95']:.2f}, P99 {d['p99']:.2f}, max {d['max']:.2f}")


def scrub():
    names = [("footer_no_scrubber", "footer, no scrubber"), ("footer_with_scrubber", "footer, scrubber"), ("data_no_scrubber", "bit rot, no scrubber"), ("data_with_scrubber", "bit rot, scrubber")]
    rs = [load("scrubber-" + n)["results"] for n, _ in names]
    rows = [
        ["Corrupt copies injected"] + [sp(r["corrupt_copies_injected"], 0) for r in rs],
        ["Direct worker reads returning wrong bytes"] + [sp(r["direct_worker_reads_returning_wrong_bytes"], 0) for r in rs],
        ["Objects the controller served with wrong bytes"] + [sp(r["controller_objects_served_wrong_bytes"], 0) for r in rs],
        ["Objects unavailable"] + [sp(r["controller_objects_unavailable"], 0) for r in rs],
        ["SHA-256 mismatches hit by readers"] + [sp(r["sha256_mismatches_hit_by_readers"], 0) for r in rs],
        ["Copies quarantined by the scrubber"] + [sp(r["copies_quarantined_by_scrubber"], 0) for r in rs],
        ["Corrupt copies still on disk afterwards"] + [sp(r["corrupt_copies_still_on_disk"], 0) for r in rs],
        ["Slowest scrubber pass, ms"] + [sp(r["slowest_scrubber_pass_ms"]) for r in rs],
    ]
    for lab, k in [("P50", 0), ("P95", 1), ("P99", 2), ("max", 3)]:
        rows.append([f"Object read ms, {lab} (n={rs[0]['object_read_ms_after_corruption']['pooled']['n']})"] + [dist(r["object_read_ms_after_corruption"]["pooled"], 2)[k] for r in rs])
    print(table(["Metric (median over reps, min-max)"] + [t for _, t in names], rows))
    p = load("scrubber-pass_duration")["results"]
    print()
    print(table(["Pause between chunks", "Seconds for 1000 x 1 MiB chunks", "Chunks/s"], [["none", sp(p["no_pause_seconds"], 2), sp(p["no_pause_chunks_per_s"], 0)], ["10 ms (default)", sp(p["pause_10ms_seconds"], 2), num(1000 / p["pause_10ms_seconds"]["median"], 0)]]))
    print(f"\nCorrupt chunks found per pass: {sp(p['corrupt_found'], 0)} of 10")


def hedge():
    r = load("hedge-delays")["results"]
    on, off = r["hedging_on_25ms"], r["hedging_off"]
    rows = []
    for a, b in zip(off, on):
        rows.append([f"{a['slow_worker_delay_ms']:.0f} ms"] + dist(a["read_ms"]["pooled"]) + dist(b["read_ms"]["pooled"]) + [f"{b['hedge_requests_all_reps']} of {b['reads_all_reps']}", f"{a['read_ms']['pooled']['p99'] / b['read_ms']['pooled']['p99']:.1f}x"])
    print(table(["Slow-worker delay", "Off P50", "Off P95", "Off P99", "Off max", "On P50", "On P95", "On P99", "On max", "Hedges fired", "P99 off / on"], rows))
    print(f"\n(all in ms; n={on[0]['read_ms']['pooled']['n']} reads per cell; per-repetition P99 spread, hedging on: " + ", ".join(f"{c['slow_worker_delay_ms']:.0f} ms: {sp(c['read_ms']['p99_across_reps'])}" for c in on) + "; hedging off: " + ", ".join(f"{c['slow_worker_delay_ms']:.0f} ms: {sp(c['read_ms']['p99_across_reps'])}" for c in off) + ")")
    s = load("hedge-threshold_sweep")["results"]
    rows = []
    for c in s:
        rows.append(["off" if c["hedge_threshold_ms"] == 0 else f"{c['hedge_threshold_ms']} ms"] + dist(c["read_ms"]["pooled"]) + [f"{c['hedge_requests_all_reps']} of {c['reads_all_reps']} ({c['hedge_rate_pct']:.1f}%)", sp(c["read_ms"]["p99_across_reps"])])
    print()
    print(table(["Threshold", "P50", "P95", "P99", "max", "Hedges fired", "P99 per rep (median, min-max)"], rows))


def heal():
    rs = load("reconciler")["results"]
    rows = []
    for r in rs:
        rows.append([f"{r['chunks_lost_by_restarted_worker']} x {r['chunk_kib']} KiB", sp(r["chunks_with_one_copy_before"], 0), sp(r["chunks_with_one_copy_after_wait_without_reconciler"], 0), sp(r["reconciler_pass_ms"], 0), sp(r["chunks_per_s"], 0), sp(r["mib_per_s"], 1), sp(r["chunks_with_two_copies_after"], 0), sp(r["objects_unreadable_after"], 0)])
    print(table(["Chunks lost", "One copy before", "One copy after a 1 s wait, no reconciler", "Pass ms", "Chunks/s", "MiB/s", "Two copies after the pass", "Objects unreadable"], rows))


def push():
    r = load("compounding")["results"]
    on, off = r["dedup_on"], r["dedup_off"]
    rows = [
        ["Push 1 ms (n=%d)" % on["first_push_ms"]["n"], f"P50 {on['first_push_ms']['p50']:.0f}, max {on['first_push_ms']['max']:.0f}", f"P50 {off['first_push_ms']['p50']:.0f}, max {off['first_push_ms']['max']:.0f}"],
    ]
    for lab, k in [("P50", 0), ("P95", 1), ("P99", 2), ("max", 3)]:
        rows.append([f"Pushes 2-10 ms, {lab} (n={on['pushes_2_to_10_ms']['pooled']['n']})", dist(on["pushes_2_to_10_ms"]["pooled"], 0)[k], dist(off["pushes_2_to_10_ms"]["pooled"], 0)[k]])
    rows += [
        ["MiB sent, first push", sp(on["first_push_sent_mib"], 0), sp(off["first_push_sent_mib"], 0)],
        ["MiB sent, each later push", sp(on["each_later_push_sent_mib"], 0), sp(off["each_later_push_sent_mib"], 0)],
        ["MiB sent, all 10 pushes", sp(on["total_sent_mib"], 0), sp(off["total_sent_mib"], 0)],
        ["Seconds, all 10 pushes", sp(on["total_seconds_10_pushes"], 2), sp(off["total_seconds_10_pushes"], 2)],
    ]
    print(table(["Metric", "Dedup on", "Dedup off"], rows))
    print(f"\nTotal time off / on: {ratio(r['total_time_off_over_on'])}; median later push off / on: {ratio(r['later_push_median_off_over_on'])}; bytes off / on: {r['total_bytes_off_over_on']:.2f}x")


def media():
    r = load("random-media")["results"]
    on, off = r["dedup_on"], r["dedup_off"]
    rows = []
    for lab, k in [("P50", 0), ("P95", 1), ("P99", 2), ("max", 3)]:
        rows.append([f"Upload ms per 5 MiB file, {lab} (n={on['upload_ms_per_5mib_file']['pooled']['n']})", dist(on["upload_ms_per_5mib_file"]["pooled"])[k], dist(off["upload_ms_per_5mib_file"]["pooled"])[k]])
    rows += [["Upload ms per chunk (median over reps, min-max)", sp(on["ms_per_chunk"]), sp(off["ms_per_chunk"])], ["Stored (MiB)", sp(on["stored_mib"], 0), sp(off["stored_mib"], 0)], ["Storage ratio", sp(on["storage_ratio"], 2), sp(off["storage_ratio"], 2)]]
    print(table(["Metric", "Dedup on", "Dedup off"], rows))
    print(f"\nPer-chunk time off / on: {ratio(r['per_chunk_time_off_over_on'])}")


def ring():
    r = load("ring")["results"]
    rows = []
    for m in r["movement"]:
        rows.append([f"{m['From']} to {m['To']}", f"{m['ideal_pct']:.1f}%", sp(m["ring_primary_moved_pct"]) + "%", sp(m["modulo_primary_moved_pct"]) + "%", sp(m["ring_primary_or_secondary_changed_pct"]) + "%"])
    print(table(["Workers", "Ideal", "Consistent hashing (primary moved)", "Modulo (primary moved)", "Ring: primary or secondary changed"], rows))
    print()
    rows = []
    for b in r["balance"]:
        rows.append([b["workers"], f"{b['fair_share_pct']:.1f}%", sp(b["min_share_pct"]) + "%", sp(b["max_share_pct"]) + "%", sp(b["coefficient_of_variation_pct"]) + "%"])
    print(table(["Workers", "Fair share", "Smallest share", "Largest share", "Spread (CV)"], rows))


SECTIONS = {"env": env, "dedup": dedup, "avail": avail, "scrub": scrub, "hedge": hedge, "heal": heal, "push": push, "media": media, "ring": ring}
for k, f in SECTIONS.items():
    if not ONLY or ONLY == k:
        print(f"\n<!-- {k} -->")
        f()
