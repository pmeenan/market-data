# Strategy research and opportunity scanning

D-036 records the owner's 2026-09-04 direction: identify overcorrection/recovery
opportunities targeting approximately 1% moves over hours or days, then apply
validated strategies in an ongoing read-only scanner. This is the acceptance
contract for M3–M6, not a claim that the following features are implemented.
The event runner and immutable catalog exist; the first study, execution
model, validation workflow, and scanner remain pending in [plan.md](plan.md).

## Account and return assumptions

The owner trades in a 401(k) with no trading fees. Default commissions and
explicit trading fees to zero, and omit per-trade capital-gains tax deductions
from the account-return simulation. This is the owner's modeling context, not
an inference about every retirement plan or withdrawal taxation.

Keep spread and slippage separate from fees. Publish a frictionless reference
and configurable execution-price sensitivities; do not impose an arbitrary
20-basis-point charge as the baseline. Record whether costs are per side or
round trip and avoid double counting spread already embedded in bid/ask fills.
Calibrate assumptions from timestamped shadow observations when available.
Zero commissions do not establish zero execution-price friction; see the
[SEC's execution explanation](https://www.investor.gov/introduction-investing/investing-basics/how-stock-markets-work/executing-order).

Initially model long-only, unlevered positions as a conservative research
assumption. Before portfolio validation, record the owner's actual permitted
instruments, order types, available capital, cash-reuse/settlement policy,
position limits, and any plan-specific trading restrictions. Do not assume
margin, shorting, or unrestricted immediate reuse of proceeds, and do not
invent universal 401(k) rules. Missing account details need not block event
research; portfolio outputs must label unverified assumptions.

## Prices and information availability (M3/M4)

- Use consistent raw prices for same-session entry/exit returns. Adjusted EOD
  prices may support comparable historical gap features, but never divide raw
  intraday prices by adjusted EOD prices. Declare a common basis for every
  cross-session return; account explicitly for splits, cash dividends, and
  share quantities when simulating holdings. Distinguish total return from
  tradable price recovery and flag ex-dividend/split events.
- Test historical adjustment restatements, actual split/ex-dividend boundaries,
  unchanged-price cases, and hand-calculated gains/losses. A common adjustment
  multiplier fixture alone does not validate holding across a corporate action.
- Record bar start, bar end, decision time, and feature availability. A direct
  hourly 10:00 bar's close is available at 11:00; a 09:30 five-minute bar is
  complete at 09:35. Use the calendar for DST and early-close boundaries.
- A completed-bar signal enters only after its features are available plus the
  declared delay. An observed opening price is not an assumed executable entry
  at that same opening print. Model publication latency independently of labels.
- Build one reusable as-of feature path for backtests and scanning. EOD
  liquidity uses prior completed sessions. Event-day open is an explicitly
  timestamped feature; event-day final high/low/close/volume are unavailable to
  a morning signal. Every join and rolling window must preserve these rules.
- The current runner restricts dataset visibility and audits declared lookbacks;
  it does **not** sandbox selection callbacks against future rows in those
  datasets. Until the shared as-of path lands, study callbacks own causality.
  Perturb future OHLCV and outcome availability and prove earlier signals do
  not change. Test same-day final fields and unfinished bars as well.
- Latest revised histories are not historical publication vintages. Existing
  manifests detect changes but do not restore old inputs. Archive decision
  features and strategy code/version for promoted strategies; snapshot required
  research inputs when exact reruns must survive warehouse corrections.

## Executable strategy evaluation (M5)

Keep descriptive M3/M4 event results separate from simulated trades. A focused
execution model must declare decision/entry delay, fill convention, target,
stop, maximum holding period, session/overnight policy, sizing, and costs.
Define whether the nominal 1% target is gross price movement or net return.
No market order is submitted by this simulation.

For five-minute bars touching both stop and target, preserve an ambiguous
status and publish conservative and optimistic bounds. An OHLC touch does not
prove a limit fill. Model gap-through-stop exits at the next executable price,
not automatically at the stop. Preserve missing outcomes, halts, stale bars,
and terminal/delisted holdings explicitly; do not assume liquidation at the
last stored close. Use finer data only if measured ambiguity warrants a new
scope decision.

Report gross/net expectancy, median and tail returns, win/loss sizes,
target-before-stop frequency, time to target, maximum adverse/favorable
excursion, ambiguous-fill counts, and missing outcomes. For a portfolio add
cash/position accounting, deterministic simultaneous-signal priority, exposure
and concentration limits, overlapping holdings, capital utilization, turnover,
and drawdown. Event averages alone cannot establish portfolio performance.

## Validation and representativeness (M3–M5)

Before searching parameters, freeze chronological development, validation, and
untouched test periods, plus the selection/promotion criteria. M3/M4 may
publish a negative or inconclusive result; milestone completion never requires
finding a profitable strategy. M5 promotion requires the predeclared criteria.

Record every trial, including failures and rejected parameter sets, in the
existing catalog. Use walk-forward evaluation and exclude training events whose
outcome windows cross a validation boundary. Allow legitimate past lookbacks
for validation features. Once a test period informs tuning, it is no longer
untouched. Report parameter stability and uncertainty using day/time-block
resampling long enough to reflect overlapping holdings, not independent-row
confidence intervals for correlated events. See the research on
[backtest overfitting](https://www.davidhbailey.com/dhbpapers/backtest-prob.pdf).

Publish a coverage funnel from EOD candidates through local intraday
eligibility, selected events, and evaluable/ambiguous/missing outcomes. Break
exclusions and results down by year, liquidity, stock/ETF, and active/delisted
status. Historical intraday data is seed-dependent: broad EOD and stable ids
alone do not remove intraday sample bias. Keep missing outcomes in denominators
and show explicit favorable/adverse sensitivity scenarios with their assumed
bounds; do not treat them as known zero returns.

Measure session-filtered zero-volume/stale bars and cross-frequency consistency
on the actual cohort. Validate trigger prices against timestamped shadow
observations; retain source labels. IEX volume is not composite liquidity.

Start with lagged volatility-normalized gaps, prior trend, composite EOD
liquidity, and contemporaneously available market-relative moves. Separate
stocks and ETFs. Compare with SPY and controls matched using decision-time
features only. Evaluate earnings/corporate-action flags when point-in-time
coverage can be established; unknown event status must remain unknown. A new
source requires a source decision. Evaluate performance by market regime and
remove dependence on a few names/dates before promotion.

## Read-only opportunity workflow (M6)

Use nightly completed EOD data to generate a ranked candidate list, followed
by bounded daytime five-minute evaluation through the **same** features and
signal function used in replay. The initial strategy defines its required
monitoring hours, which may extend beyond the morning. Preserve D-031's
no-daytime-broad-collector policy: measure whether the bounded candidate set
captures unexpected opening gaps; record missed opportunities and coverage.
If broad daytime discovery is required, explicitly revisit scope and budgets.

Before enabling collection, record the Tiingo endpoint, independently validated
identity, cadence/budget, freshness threshold, source/receipt timestamps,
partial-bar policy, credentials, and separate append-only observation layout.
A broker feed still requires separate approval. Scanner observations cannot
silently become canonical bars or block overnight collection for a session.

Persist signal id, stable instrument id, strategy version/code fingerprint,
parameters, input feature values and timestamps, price basis, decision time,
reason, expiry, and source/freshness. Deduplicate by strategy/instrument/decision
and define cooldown/re-arm rules. Distinguish no signal, stale-data suppression,
unsupported instrument, and scanner failure. CLI output is sufficient initially;
a web UI remains optional, and external delivery channels need their own scope.

Shadow-run without orders. Retain first-seen inputs so replay comparisons do
not substitute corrected later bars. Compare signal equality, arrival latency,
missed/duplicate alerts, coverage, and simulated outcomes under the frozen
execution policy. Predeclare a representative duration/event count and acceptable
error thresholds before the trial; do not choose the endpoint because early
results look favorable. No shadow order touch is claimed as a proven fill.
