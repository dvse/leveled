# AQ-B B12 — leveled-fork raw layer benchmark (index fold + head fetch).
#
# Isolates the storage layer from the AshLeveled planner / AshQueue upper
# layers by driving leveled_bookie directly. Run from the ash_queue project so
# the :leveled application and the AshQueue.Bench.ForkBench helper are loaded:
#
#   cd ~/projects/agents/ash_queue
#   MIX_ENV=test mix run ../leveled/bench/aq_index_fold_bench.exs
#
# Gate (B12): >= 50k index entries/sec on a 500k-entry 2i index; head p95 < 1ms.
# Does NOT modify fork source.

n = System.get_env("AQ_B12_N", "500000") |> String.to_integer()
result = AshQueue.Bench.ForkBench.run(n: n)
IO.puts("B12 RESULT: #{inspect(result, pretty: true)}")

out = System.get_env("AQ_B12_OUT", "/tmp/aq_b12_result.exs")
File.write!(out, inspect(result, pretty: true))
IO.puts("wrote #{out}")
