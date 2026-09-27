
### Scheduled monthly slots and retry barrier

For a scheduled quarter, the three mapped months receive earliest extraction
starts at selection time, initially separated by one hour. All maps belong to
one DagRun and retain its single logical date. Slots are lower bounds; worker
availability, CDS queueing, context chunks, and the global PostgreSQL pacing
clock can delay actual starts and completion. The CDS clock has a separate
30-minute operational default and applies to every actual submission in both
scheduled and manual modes. Neither spacing value is a Copernicus requirement.
Manual recovery receives no scheduled start offset.

The `first_extract_attempt` map gives each month one attempt (including
worker-free pacing deferrals). It records a real failure and emits bounded
control metadata. `prepare_retry_pass` waits for all first-pass maps; then
`extract_or_resume` passes through successes and retries failed partitions with
one Airflow retry, reusing validated checkpoints. Together with the first
pass, this preserves the original maximum of three extraction attempts. Terminal failures
remain failed in durable progress and block the next quarter until verified
manual repair. Transform and load follow the resolved extraction results.

Before resuming the campaign, check the Airflow UI or CLI for **active and
queued DagRuns** and their mapped task states. Pausing the DAG prevents new
scheduled runs but does not stop an already running task. Run the offline
Airflow suite, inspect the 2016 Q1 campaign report and the three database
partitions, and keep the DAG paused until that evidence and a post-change pilot
are reviewed. The observed first run took 2h 45m 52s, above the configured
2-hour tick; `max_active_runs=1` prevents overlap but can leave ticks queued
or delayed. Review more than one runtime and the queue before adjusting cadence.
