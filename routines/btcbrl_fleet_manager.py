"""Top-level discovery shim for the BTC-BRL fleet manager.

condor's discover_routines only scans the top-level routines/ folder, so this re-exports the
real routine that lives in the agent package (agents/btcbrl_fleet/routines/), whose __file__
keeps the spec path (../spec.json) working. Edit the real one, not this shim.
"""
from agents.btcbrl_fleet.routines.btcbrl_fleet_manager import CONTINUOUS, Config, run  # noqa: F401
