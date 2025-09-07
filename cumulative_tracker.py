#!/usr/bin/env python3
"""
Cumulative enrichment tracker for Mangle processor
Tracks enrichment progress across multiple runs
"""
import json
import os
from datetime import datetime

def load_cumulative_state():
    """Load cumulative enrichment state"""
    try:
        with open("cumulative_enrichment_state.json", "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        # Initialize cumulative state
        return {
            "timestamp": datetime.now().isoformat(),
            "total_records_processed": 0,
            "source_counts_cumulative": {
                "LIBRARY_OF_CONGRESS": 0,
                "GOOGLE_BOOKS": 0,
                "VERTEX_AI": 0,
                "OPEN_LIBRARY": 0,
                "NO_ENRICHMENT": 809  # Start with all records unenriched
            },
            "runs_completed": 0,
            "overall_completion_percentage": 0.0
        }

def update_cumulative_state(current_run_state):
    """Update cumulative state with current run data"""
    cumulative = load_cumulative_state()
    
    # Update cumulative counts - ACCUMULATE values from current run
    for source, count in current_run_state["source_counts"].items():
        if source in cumulative["source_counts_cumulative"]:
            # Only accumulate positive values (ignore negative NO_ENRICHMENT)
            if count > 0:
                cumulative["source_counts_cumulative"][source] += count
    
    # Update totals - use maximum of current or cumulative
    cumulative["total_records_processed"] = max(cumulative["total_records_processed"], current_run_state["total_records"])
    cumulative["runs_completed"] += 1
    
    # Calculate overall completion based on records processed, not source counts
    # Each record can be enriched by multiple sources, so we use total_records_processed
    cumulative["overall_completion_percentage"] = (cumulative["total_records_processed"] / 809) * 100
    cumulative["timestamp"] = datetime.now().isoformat()
    
    # Recalculate NO_ENRICHMENT properly for cumulative state
    # Use the actual total records processed, not sum of source counts
    cumulative["source_counts_cumulative"]["NO_ENRICHMENT"] = 809 - cumulative["total_records_processed"]
    
    # Save cumulative state
    with open("cumulative_enrichment_state.json", "w") as f:
        json.dump(cumulative, f, indent=2)
    
    return cumulative

def get_cumulative_progress():
    """Get current cumulative progress"""
    return load_cumulative_state()

if __name__ == "__main__":
    # Test the cumulative tracker
    try:
        with open("mangle_enrichment_state.json", "r") as f:
            current_state = json.load(f)
        cumulative = update_cumulative_state(current_state)
        print("Cumulative state updated:")
        print(json.dumps(cumulative, indent=2))
    except Exception as e:
        print(f"Error: {e}")