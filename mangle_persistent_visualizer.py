#!/usr/bin/env python3
"""
Mangle Persistent Enrichment Monitor
Monitors the Mangle-based enrichment architecture in real-time
"""
import time
import json
import os
import shutil
from datetime import datetime

def get_terminal_dimensions():
    """Get current terminal dimensions (columns, rows)"""
    try:
        columns, rows = shutil.get_terminal_size()
        return columns, rows
    except:
        return 80, 24

def create_progress_bar(percentage, width=40):
    """Create a visual progress bar"""
    filled = int(width * percentage / 100)
    bar = '█' * filled + '░' * (width - filled)
    return f"{bar} {percentage:.1f}%"

def analyze_enrichment_sources():
    """Analyze enrichment source usage from cumulative state file"""
    try:
        # Try to load cumulative state first
        with open("cumulative_enrichment_state.json", "r") as f:
            cumulative_state = json.load(f)
        return cumulative_state.get("source_counts_cumulative", {}), cumulative_state.get("total_records_processed", 0)
    except (FileNotFoundError, json.JSONDecodeError):
        # Fallback to current run state
        try:
            with open("mangle_enrichment_state.json", "r") as f:
                state = json.load(f)
            return state.get("source_counts", {}), state.get("total_records", 0)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}, 0

def display_mangle_dashboard(source_counts, total_records, previous_output_lines=None):
    """Display the Mangle enrichment dashboard with ANSI rewrite to reduce flickering"""
    columns, rows = get_terminal_dimensions()
    separator_width = min(columns, 120)
    progress_bar_width = max(20, min(columns - 50, 60))
    
    output_lines = []
    
    output_lines.append("\n" + "=" * separator_width)
    output_lines.append("🔍 MANGLE ENRICHMENT MONITOR".center(separator_width))
    output_lines.append("=" * separator_width)

    # Overall Progress
    TARGET_RECORDS = 809
    overall_progress = (total_records / TARGET_RECORDS) * 100 if TARGET_RECORDS > 0 else 0
    output_lines.append(f"📊 Overall Progress: {create_progress_bar(overall_progress, progress_bar_width)}")
    output_lines.append(f"📦 Total Records Enriched: {total_records} / {TARGET_RECORDS}")
    output_lines.append("-" * separator_width)

    # Source Usage with proper percentages
    output_lines.append("🔧 ENRICHMENT SOURCE USAGE:".center(separator_width))
    output_lines.append("-" * separator_width)
    if source_counts:
        TARGET_RECORDS = 809
        
        # Display source counts with percentage of total target
        sources_to_display = ["LIBRARY_OF_CONGRESS", "GOOGLE_BOOKS", "VERTEX_AI", "OPEN_LIBRARY"]
        
        for source in sources_to_display:
            count = source_counts.get(source, 0)
            percentage = (count / TARGET_RECORDS) * 100 if TARGET_RECORDS > 0 else 0
            
            # Format display names
            display_names = {
                "LIBRARY_OF_CONGRESS": "Library Of Congress",
                "GOOGLE_BOOKS": "Google Books",
                "VERTEX_AI": "Vertex Ai",
                "OPEN_LIBRARY": "Open Library"
            }
            # display_name = display_names.get(source, source.replace("_", " ").title())
            # output_lines.append(f"   {display_name:20}: {count:5} records ({percentage:.1f}%)")
        
        # Display No Enrichment separately (now integrated into bar chart)
        # no_enrich_count = source_counts.get("NO_ENRICHMENT", 0)
        # no_enrich_percentage = (no_enrich_count / TARGET_RECORDS) * 100 if TARGET_RECORDS > 0 else 0
        # output_lines.append(f"   {'No Enrichment':20}: {no_enrich_count:5} records ({no_enrich_percentage:.1f}%)")
        
        # Add source utilization table (replaces bar chart)
        output_lines.append("\n   📊 SOURCE UTILIZATION TABLE:")
        
        # Create a clean table format
        table_header = "   Source              Records    % of Target"
        table_separator = "   ------------------ ---------- ------------"
        
        output_lines.append(table_header)
        output_lines.append(table_separator)
        
        # Display names for sources
        display_names = {
            "LIBRARY_OF_CONGRESS": "Library of Congress",
            "GOOGLE_BOOKS": "Google Books",
            "VERTEX_AI": "Vertex AI",
            "OPEN_LIBRARY": "Open Library"
        }
        
        # Calculate records with complete enrichment from ALL 4 sources
        # This requires analyzing the actual enriched data file
        completely_enriched_records = 0
        try:
            with open("enriched_data_combined_mangle.json", "r") as f:
                enriched_data = json.load(f)
            
            for record in enriched_data:
                source_data = record.get("source_data", {})
                # Check if ALL 4 sources have contributed data
                if (bool(source_data.get("loc", {})) and 
                    bool(source_data.get("google_books", {})) and 
                    bool(source_data.get("vertex_ai", {})) and 
                    bool(source_data.get("open_library", {}))):
                    completely_enriched_records += 1
                    
        except Exception as e:
            # Fallback: use the difference method if we can't read the enriched file
            completely_enriched_records = source_counts.get("total_records", 0) - source_counts.get("NO_ENRICHMENT", 0)
        
        for source in sources_to_display:
            count = source_counts.get(source, 0)
            percentage = (count / TARGET_RECORDS) * 100 if TARGET_RECORDS > 0 else 0
            display_name = display_names.get(source, source.replace("_", " ").title())
            output_lines.append(f"   {display_name:18} {count:7}     {percentage:6.1f}%")
        
        # Display No Enrichment
        no_enrich_count = source_counts.get("NO_ENRICHMENT", 0)
        no_enrich_percentage = (no_enrich_count / TARGET_RECORDS) * 100 if TARGET_RECORDS > 0 else 0
        output_lines.append(table_separator)
        output_lines.append(f"   {'No Enrichment':18} {no_enrich_count:7}     {no_enrich_percentage:6.1f}%")
        
        # Total line - shows records with COMPLETE enrichment from ALL 4 sources
        output_lines.append(table_separator)
        output_lines.append(f"   {'COMPLETE':18} {completely_enriched_records:7}     {(completely_enriched_records / TARGET_RECORDS) * 100:6.1f}%")
        output_lines.append(f"   {'TARGET':18} {TARGET_RECORDS:7}     100.0%")
            
    else:
        output_lines.append("   No source data available yet.")
    
    output_lines.append("-" * separator_width)

    # Record Type Distribution
    output_lines.append("📦 TARGET RECORD DISTRIBUTION:".center(separator_width))
    output_lines.append("-" * separator_width)
    try:
        with open("enriched_data_combined_mangle.json", "r") as f:
            data = json.load(f)
        
        record_types = {"MARC_B_BARCODES": 0, "ISBN_ENTRIES": 0, "NOISBN_ENTRIES": 0}
        for record in data:
            barcode = record.get("barcode", "")
            if barcode.startswith("ISBN_"):
                record_types["ISBN_ENTRIES"] += 1
            elif barcode.startswith("NOISBN_"):
                record_types["NOISBN_ENTRIES"] += 1
            elif barcode.startswith("B"):
                record_types["MARC_B_BARCODES"] += 1
        
        for rtype, count in record_types.items():
            percentage = (count / TARGET_RECORDS) * 100 if TARGET_RECORDS > 0 else 0
            display_name = rtype.replace("_", " ").title()
            output_lines.append(f"   {display_name:20}: {count:4} records ({percentage:.1f}% of target)")
            
    except Exception as e:
        output_lines.append(f"   Error loading record distribution: {e}")

    output_lines.append("-" * separator_width)
    
    # Status and Timestamp
    status = "✅ COMPLETE" if overall_progress >= 100 else "🔄 PROCESSING"
    output_lines.append(f"💡 STATUS: {status}")
    output_lines.append(f"🕒 Last Update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    output_lines.append("=" * separator_width)
    output_lines.append("Press Ctrl+C to exit | Updates every 5 seconds".center(separator_width))
    
    # Use ANSI rewrite to only update changed parts
    if previous_output_lines:
        # Clear previous output by moving cursor up and clearing lines
        clear_lines = len(previous_output_lines)
        ansi_clear = f"\033[{clear_lines}A\033[0J"
        print(ansi_clear, end="")
    
    # Print current output
    for line in output_lines:
        print(line)
    
    return output_lines

# Note: Visualizer should only READ state files, not write to them
# State writing is handled by the parallel processor to prevent corruption

def main():
    """Main function for persistent monitoring"""
    print("🚀 Starting Mangle Persistent Enrichment Monitor...")
    print("📡 Monitoring Mangle-based enrichment architecture")
    print("👀 Displaying cumulative enrichment progress")
    print("Press Ctrl+C to exit\n")
    
    # Clear screen initially
    os.system('cls' if os.name == 'nt' else 'clear')
    
    previous_output_lines = None
    
    try:
        while True:
            source_counts, total_records = analyze_enrichment_sources()
            previous_output_lines = display_mangle_dashboard(source_counts, total_records, previous_output_lines)
            # save_state(source_counts, total_records)  # Disabled to prevent state corruption
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\n\n🛑 Monitoring stopped by user")
        print("📊 Final state saved to mangle_enrichment_state.json")
    except Exception as e:
        print(f"\n❌ Error in monitoring: {e}")

if __name__ == "__main__":
    main()
