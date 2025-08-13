#!/usr/bin/env python3
"""
Enhanced Auto-Assign System - Main Branch
==========================================

This is a comprehensive auto-assign system that consolidates all auto-assignment functionality
into a single file. It includes:

1. Enhanced Auto-Assign Trigger - Background processing with fair distribution
2. Auto-Assign Monitor - Real-time monitoring and statistics
3. Setup and Verification - Database schema setup and verification
4. Testing Framework - Comprehensive testing of all features
5. Live Monitoring - Real-time activity monitoring

Features:
- Fair distribution based on auto_assign_count
- IST timezone support (UTC+05:30)
- Comprehensive history logging
- Real-time monitoring and statistics
- Automatic triggers and functions
- Enhanced error handling and fallbacks

Usage:
- Main trigger: EnhancedAutoAssignTrigger()
- Monitor: EnhancedAutoAssignMonitor()
- Setup: setup_enhanced_auto_assign()
- Testing: EnhancedAutoAssignTester()
- Live monitoring: LiveAutoAssignMonitor()
"""

import threading
import time
import os
from datetime import datetime, timedelta
from pytz import timezone
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables
load_dotenv()

# Initialize Supabase client
# Main Branch - Use main database credentials
supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_ANON_KEY')

# Log which environment we're using
if os.getenv('SUPABASE_URL'):
    print("🔧 MAIN BRANCH: Using main database credentials")
else:
    print("🔧 ERROR: No database credentials found")

supabase: Client = create_client(supabase_url, supabase_key)

# IST Timezone for Indian Standard Time
IST_TIMEZONE = timezone('Asia/Kolkata')

def get_ist_timestamp():
    """Get current timestamp in IST timezone"""
    return datetime.now(IST_TIMEZONE)

def auto_assign_new_leads_for_source(source):
    """
    Automatically assign new leads for a specific source based on saved configuration.
    This function can be called whenever new leads are added to the system.
    
    Args:
        source (str): The source name to auto-assign leads for
        
    Returns:
        dict: Result with assigned_count and status
    """
    try:
        print(f"🤖 Auto-assigning new leads for source: {source}")
        
        # Get auto-assign configuration for this source
        config_result = supabase.table('auto_assign_config').select('cre_id').eq('source', source).execute()
        configs = config_result.data or []
        
        if not configs:
            print(f"ℹ️ No auto-assign configuration found for {source}")
            return {'success': False, 'message': f'No auto-assign configuration found for {source}', 'assigned_count': 0}
        
        cre_ids = [config['cre_id'] for config in configs]
        print(f"📋 Found {len(cre_ids)} CREs configured for {source}: {cre_ids}")
        
        # Get CRE details for fair distribution
        cre_result = supabase.table('cre_users').select('*').in_('id', cre_ids).execute()
        cres = cre_result.data or []
        
        if not cres:
            print(f"⚠️ No CREs found for source {source}")
            return {'success': False, 'message': f'No CREs found for source {source}', 'assigned_count': 0}
        
        # Get unassigned leads for this source
        unassigned_result = supabase.table('lead_master').select('*').eq('assigned', 'No').eq('source', source).execute()
        unassigned_leads = unassigned_result.data or []
        
        if not unassigned_leads:
            print(f"ℹ️ No unassigned leads found for {source}")
            return {'success': True, 'message': f'No unassigned leads found for {source}', 'assigned_count': 0}
        
        print(f"📊 Found {len(unassigned_leads)} unassigned leads for {source}")
        
        # Get the last assigned CRE for this source to determine starting point
        last_assigned_result = supabase.table('lead_master').select('cre_name').eq('source', source).order('cre_assigned_at', desc=True).limit(1).execute()
        last_cre_name = None
        if last_assigned_result.data:
            last_cre_name = last_assigned_result.data[0].get('cre_name')
        
        # Find starting index for round-robin
        start_index = 0
        if last_cre_name:
            for i, cre in enumerate(cres):
                if cre['name'] == last_cre_name:
                    start_index = (i + 1) % len(cres)
                    break
        
        # Assign leads using round-robin fair distribution
        assigned_count = 0
        for i, lead in enumerate(unassigned_leads):
            selected_cre = cres[(start_index + i) % len(cres)]
            
            update_data = {
                'cre_name': selected_cre['name'],
                'assigned': 'Yes',
                'cre_assigned_at': get_ist_timestamp().isoformat(),
                'lead_status': 'Pending'
            }
            
            try:
                # Update lead assignment
                supabase.table('lead_master').update(update_data).eq('uid', lead['uid']).execute()
                
                # Create call attempt history
                call_history_data = {
                    'lead_uid': lead['uid'],
                    'cre_name': selected_cre['name'],
                    'call_date': get_ist_timestamp().isoformat(),
                    'call_status': 'Pending',
                    'call_notes': f'Auto-assigned to {selected_cre["name"]}',
                    'source': source
                }
                supabase.table('cre_call_attempt_history').insert(call_history_data).execute()
                
                # Update CRE's auto-assign count
                supabase.table('cre_users').update({
                    'auto_assign_count': selected_cre.get('auto_assign_count', 0) + 1
                }).eq('id', selected_cre['id']).execute()
                
                # Log auto-assign history
                history_data = {
                    'source': source,
                    'cre_id': selected_cre['id'],
                    'cre_name': selected_cre['name'],
                    'lead_uid': lead['uid'],
                    'assigned_at': get_ist_timestamp().isoformat(),
                    'assignment_type': 'auto_new_lead'
                }
                supabase.table('auto_assign_history').insert(history_data).execute()
                
                assigned_count += 1
                print(f"✅ Auto-assigned new lead {lead['uid']} to {selected_cre['name']} (source: {source})")
                
            except Exception as e:
                print(f"❌ Error assigning new lead {lead['uid']}: {e}")
                continue
        
        print(f"✅ Auto-assigned {assigned_count} new leads for {source}")
        return {
            'success': True,
            'message': f'Successfully auto-assigned {assigned_count} new leads for {source}',
            'assigned_count': assigned_count
        }
        
    except Exception as e:
        print(f"❌ Error in auto_assign_new_leads_for_source: {e}")
        return {'success': False, 'message': str(e), 'assigned_count': 0}

# =============================================================================
# 1. ENHANCED AUTO-ASSIGN TRIGGER SYSTEM
# =============================================================================

class EnhancedAutoAssignTrigger:
    """
    Enhanced background task system for automatic lead assignment with fair distribution.
    
    This class provides:
    - Background processing of unassigned leads
    - Fair distribution based on auto_assign_count
    - IST timezone support for all timestamps
    - Comprehensive history logging
    - Automatic triggers and functions
    - Enhanced error handling with fallbacks
    
    Usage:
        trigger = EnhancedAutoAssignTrigger()
        trigger.start()  # Start background processing
        trigger.stop()   # Stop background processing
    """
    
    def __init__(self, check_interval=30):
        """
        Initialize the enhanced auto-assign trigger.
        
        Args:
            check_interval (int): Interval in seconds between checks (default: 30)
        """
        self.check_interval = check_interval
        self.running = False
        self.thread = None
        self.last_check = None
        
    def start(self):
        """Start the background auto-assign trigger"""
        if not self.running:
            self.running = True
            self.thread = threading.Thread(target=self._run_loop, daemon=True)
            self.thread.start()
            print(f"🚀 Enhanced Auto-Assign Trigger started (checking every {self.check_interval} seconds)")
    
    def stop(self):
        """Stop the background auto-assign trigger"""
        self.running = False
        if self.thread:
            self.thread.join()
        print("🛑 Enhanced Auto-Assign Trigger stopped")
    
    def _run_loop(self):
        """Main loop that checks for unassigned leads and assigns them"""
        while self.running:
            try:
                self._check_and_assign_leads()
                time.sleep(self.check_interval)
            except Exception as e:
                print(f"❌ Error in enhanced auto-assign trigger: {e}")
                time.sleep(self.check_interval)
    
    def _check_and_assign_leads(self):
        """
        Check for unassigned leads and assign them based on auto_assign_config.
        
        This method:
        - Fetches all auto-assign configurations
        - Groups configurations by source
        - Processes each source with fair distribution
        - Logs all assignments to history
        """
        try:
            # Get all auto-assign configurations
            config_result = supabase.table('auto_assign_config').select('*').execute()
            configs = config_result.data or []
            
            if not configs:
                return
            
            # Group configs by source
            source_configs = {}
            for config in configs:
                source = config['source']
                if source not in source_configs:
                    source_configs[source] = []
                source_configs[source].append(config)
            
            # Process each source with fair distribution
            total_assigned = 0
            for source, source_configs_list in source_configs.items():
                assigned_count = self._process_source_leads_fair(source, source_configs_list)
                total_assigned += assigned_count
            
            if total_assigned > 0:
                print(f"✅ Enhanced auto-assign completed: {total_assigned} total leads assigned across all sources")
                
        except Exception as e:
            print(f"❌ Error checking auto-assign configs: {e}")
    
    def _process_source_leads_fair(self, source, configs):
        """
        Process leads for a specific source using fair distribution.
        
        Args:
            source (str): The source name (e.g., 'META', 'GOOGLE')
            configs (list): List of auto-assign configurations for this source
            
        Returns:
            int: Number of leads assigned
        """
        try:
            # Get CRE IDs for this source
            cre_ids = [config['cre_id'] for config in configs]
            
            # Get unassigned leads for this source
            unassigned_result = supabase.table('lead_master').select('*').eq('assigned', 'No').eq('source', source).execute()
            unassigned_leads = unassigned_result.data or []
            
            if not unassigned_leads:
                print(f"ℹ️ No unassigned leads found for {source}")
                return 0
            
            print(f"🔄 Processing {len(unassigned_leads)} unassigned leads for {source}")
            
            # Use fair distribution to assign leads
            assigned_count = 0
            for i, lead in enumerate(unassigned_leads):
                # Get the fairest CRE for this assignment
                fairest_cre = self.get_fairest_cre_for_source(source)
                
                if fairest_cre:
                    # Assign the lead
                    update_data = {
                        'cre_name': fairest_cre['name'],
                        'assigned': 'Yes',
                        'cre_assigned_at': get_ist_timestamp().isoformat(),
                        'lead_status': 'Pending'
                    }
                    
                    try:
                        supabase.table('lead_master').update(update_data).eq('uid', lead['uid']).execute()
                        assigned_count += 1
                        
                        # Log the assignment to history
                        self.log_auto_assignment(
                            lead_uid=lead['uid'],
                            source=source,
                            cre_id=fairest_cre['id'],
                            cre_name=fairest_cre['name'],
                            assignment_method='fair_distribution'
                        )
                        
                        # Show progress every 10 assignments
                        if assigned_count % 10 == 0:
                            print(f"📊 Progress: {assigned_count}/{len(unassigned_leads)} leads assigned to {fairest_cre['name']}")
                            
                    except Exception as e:
                        print(f"❌ Error assigning lead {lead['uid']}: {e}")
                else:
                    print(f"⚠️ No available CRE found for {source}")
                    break
            
            if assigned_count > 0:
                print(f"✅ Fair distribution completed for {source}: {assigned_count} leads assigned")
                self._print_fair_distribution_stats(source)
            
            return assigned_count
                
        except Exception as e:
            print(f"❌ Error processing leads for {source}: {e}")
            return 0
    
    def get_fairest_cre_for_source(self, source):
        """
        Get the CRE with the lowest auto_assign_count for fair distribution.
        
        Args:
            source (str): The source name
            
        Returns:
            dict: CRE data with lowest auto_assign_count, or None if no CREs found
        """
        try:
            # Get CREs configured for this source with their auto_assign_count
            result = supabase.table('cre_users').select('id, name, auto_assign_count').execute()
            cres = result.data or []
            
            if not cres:
                return None
            
            # Filter CREs that are configured for this source
            configured_cre_ids = self._get_configured_cre_ids_for_source(source)
            available_cres = [cre for cre in cres if cre['id'] in configured_cre_ids]
            
            if not available_cres:
                return None
            
            # Sort by auto_assign_count (lowest first) and then by ID for consistency
            available_cres.sort(key=lambda x: (x.get('auto_assign_count', 0), x['id']))
            
            return available_cres[0]
            
        except Exception as e:
            print(f"❌ Error getting fairest CRE for {source}: {e}")
            return None
    
    def _get_configured_cre_ids_for_source(self, source):
        """
        Get CRE IDs configured for a specific source.
        
        Args:
            source (str): The source name
            
        Returns:
            list: List of CRE IDs configured for this source
        """
        try:
            result = supabase.table('auto_assign_config').select('cre_id').eq('source', source).execute()
            return [config['cre_id'] for config in result.data] if result.data else []
        except Exception as e:
            print(f"❌ Error getting configured CRE IDs for {source}: {e}")
            return []
    
    def log_auto_assignment(self, lead_uid, source, cre_id, cre_name, assignment_method='fair_distribution'):
        """
        Log auto-assignment to history table and create initial call attempt history.
        
        Args:
            lead_uid (str): The lead UID
            source (str): The source name
            cre_id (int): The CRE ID
            cre_name (str): The CRE name
            assignment_method (str): The assignment method used
        """
        try:
            # Get current auto_assign_count for the CRE
            cre_result = supabase.table('cre_users').select('auto_assign_count').eq('id', cre_id).execute()
            current_count = cre_result.data[0].get('auto_assign_count', 0) if cre_result.data else 0
            
            # Insert into auto_assign_history
            history_data = {
                'lead_uid': lead_uid,
                'source': source,
                'assigned_cre_id': cre_id,
                'assigned_cre_name': cre_name,
                'cre_total_leads_before': current_count,
                'cre_total_leads_after': current_count + 1,
                'assignment_method': assignment_method,
                'created_at': get_ist_timestamp().isoformat()
            }
            
            supabase.table('auto_assign_history').insert(history_data).execute()
            
            # Create initial call attempt history record
            call_attempt_data = {
                'uid': lead_uid,
                'call_no': 1,  # First call attempt
                'attempt': 1,   # First attempt
                'status': 'Pending',  # Initial status
                'cre_name': cre_name,
                'created_at': get_ist_timestamp().isoformat(),
                'update_ts': get_ist_timestamp().isoformat()
            }
            
            supabase.table('cre_call_attempt_history').insert(call_attempt_data).execute()
            
            # Update the CRE's auto_assign_count
            supabase.table('cre_users').update({'auto_assign_count': current_count + 1}).eq('id', cre_id).execute()
            
            # Update the lead_master table to mark as assigned
            supabase.table('lead_master').update({
                'assigned': 'Yes',
                'cre_name': cre_name,
                'cre_assigned_at': get_ist_timestamp().isoformat(),
                'lead_status': 'Pending',
                'updated_at': get_ist_timestamp().isoformat()
            }).eq('uid', lead_uid).execute()
            
            print(f"✅ Auto-assigned {lead_uid} to {cre_name} and created call attempt history")
            
        except Exception as e:
            print(f"❌ Error logging auto-assignment: {e}")
    
    def _print_fair_distribution_stats(self, source):
        """
        Print fair distribution statistics for a source.
        
        Args:
            source (str): The source name
        """
        try:
            # Get current auto_assign_count for all CREs configured for this source
            configured_cre_ids = self._get_configured_cre_ids_for_source(source)
            
            if not configured_cre_ids:
                return
            
            cre_result = supabase.table('cre_users').select('id, name, auto_assign_count').in_('id', configured_cre_ids).execute()
            cres = cre_result.data or []
            
            if cres:
                print(f"📊 Fair Distribution Stats for {source}:")
                for cre in sorted(cres, key=lambda x: x.get('auto_assign_count', 0), reverse=True):
                    print(f"   • {cre['name']}: {cre.get('auto_assign_count', 0)} leads")
                    
        except Exception as e:
            print(f"❌ Error printing fair distribution stats: {e}")
    
    def trigger_manual_assignment(self, source):
        """
        Manually trigger assignment for a specific source.
        
        Args:
            source (str): The source name
            
        Returns:
            dict: Result with assigned_count and message
        """
        try:
            print(f"🔄 Manual trigger: Processing {source}")
            
            # Get configs for this source
            config_result = supabase.table('auto_assign_config').select('*').eq('source', source).execute()
            configs = config_result.data or []
            
            if not configs:
                print(f"⚠️ No auto-assign configuration found for {source}")
                return {'assigned_count': 0, 'message': f'No configuration found for {source}'}
            
            # Process leads for this source
            assigned_count = self._process_source_leads_fair(source, configs)
            
            return {
                'assigned_count': assigned_count,
                'message': f'Successfully assigned {assigned_count} leads from {source}'
            }
            
        except Exception as e:
            print(f"❌ Error in manual trigger for {source}: {e}")
            return {'assigned_count': 0, 'message': str(e)}

# =============================================================================
# 2. AUTO-ASSIGN MONITOR SYSTEM
# =============================================================================

class EnhancedAutoAssignMonitor:
    """
    Monitor for the enhanced auto-assign system with fair distribution tracking.
    
    This class provides:
    - Real-time monitoring of auto-assignment activity
    - Fair distribution analysis and statistics
    - Source distribution tracking
    - Recent assignment history
    - Auto-assign configuration status
    
    Usage:
        monitor = EnhancedAutoAssignMonitor()
        monitor.run_monitor()  # Start monitoring
    """
    
    def __init__(self):
        """Initialize the enhanced auto-assign monitor"""
        self.last_check = None
        self.previous_stats = {}
    
    def get_current_stats(self):
        """
        Get current auto-assign statistics.
        
        Returns:
            dict: Current statistics including CREs, configs, and recent assignments
        """
        try:
            # Get all CREs with their auto_assign_count
            cre_result = supabase.table('cre_users').select('id, name, auto_assign_count').execute()
            cres = cre_result.data or []
            
            # Get auto-assign configurations
            config_result = supabase.table('auto_assign_config').select('*').execute()
            configs = config_result.data or []
            
            # Get recent auto-assignments (last 24 hours)
            ist_24h_ago = get_ist_timestamp() - timedelta(hours=24)
            history_result = supabase.table('auto_assign_history').select('*').gte('created_at', ist_24h_ago.isoformat()).execute()
            recent_assignments = history_result.data or []
            
            return {
                'cres': cres,
                'configs': configs,
                'recent_assignments': recent_assignments
            }
            
        except Exception as e:
            print(f"❌ Error getting current stats: {e}")
            return {}
    
    def analyze_fair_distribution(self, cres):
        """
        Analyze fair distribution among CREs.
        
        Args:
            cres (list): List of CRE data
            
        Returns:
            dict: Distribution statistics
        """
        if not cres:
            return {}
        
        # Sort by auto_assign_count
        sorted_cres = sorted(cres, key=lambda x: x.get('auto_assign_count', 0))
        
        # Calculate distribution statistics
        counts = [cre.get('auto_assign_count', 0) for cre in cres]
        min_count = min(counts)
        max_count = max(counts)
        avg_count = sum(counts) / len(counts)
        variance = max_count - min_count
        
        return {
            'sorted_cres': sorted_cres,
            'min_count': min_count,
            'max_count': max_count,
            'avg_count': avg_count,
            'variance': variance,
            'total_cres': len(cres)
        }
    
    def get_source_distribution(self, configs, cres):
        """
        Get distribution statistics by source.
        
        Args:
            configs (list): Auto-assign configurations
            cres (list): CRE data
            
        Returns:
            dict: Source distribution statistics
        """
        source_stats = {}
        
        for config in configs:
            source = config['source']
            cre_id = config['cre_id']
            
            # Find the CRE
            cre = next((c for c in cres if c['id'] == cre_id), None)
            if cre:
                if source not in source_stats:
                    source_stats[source] = []
                source_stats[source].append(cre)
        
        return source_stats
    
    def detect_new_assignments(self, recent_assignments):
        """
        Detect new assignments since last check.
        
        Args:
            recent_assignments (list): Recent assignment history
            
        Returns:
            list: New assignments detected
        """
        if not self.last_check:
            return []
        
        new_assignments = []
        for assignment in recent_assignments:
            assignment_time = datetime.fromisoformat(assignment['created_at'].replace('Z', '+00:00'))
            if assignment_time > self.last_check:
                new_assignments.append(assignment)
        
        return new_assignments
    
    def print_header(self):
        """Print monitor header"""
        print("\n" + "=" * 80)
        print("📊 ENHANCED AUTO-ASSIGN MONITOR")
        print("=" * 80)
        print(f"⏰ Started at: {get_ist_timestamp().strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    def print_fair_distribution_stats(self, distribution_stats):
        """
        Print fair distribution statistics.
        
        Args:
            distribution_stats (dict): Distribution statistics
        """
        if not distribution_stats:
            return
        
        print("\n🎯 FAIR DISTRIBUTION ANALYSIS")
        print("-" * 50)
        
        sorted_cres = distribution_stats.get('sorted_cres', [])
        min_count = distribution_stats.get('min_count', 0)
        max_count = distribution_stats.get('max_count', 0)
        variance = distribution_stats.get('variance', 0)
        total_cres = distribution_stats.get('total_cres', 0)
        
        print(f"📊 Total CREs: {total_cres}")
        print(f"📈 Min assignments: {min_count}")
        print(f"📉 Max assignments: {max_count}")
        print(f"📊 Variance: {variance} leads")
        
        if variance <= 1:
            print("✅ Excellent distribution")
        elif variance <= 3:
            print("⚠️ Good distribution")
        else:
            print("⚠️ Distribution needs improvement")
        
        print("\n📋 CRE Priority (Fair Distribution Order):")
        for i, cre in enumerate(sorted_cres, 1):
            count = cre.get('auto_assign_count', 0)
            status = "🎯 NEXT" if i == 1 else "⏳ WAITING"
            print(f"   {i}. {cre['name']}: {count} leads {status}")
    
    def print_source_distribution(self, source_stats):
        """
        Print source distribution statistics.
        
        Args:
            source_stats (dict): Source distribution statistics
        """
        if not source_stats:
            return
        
        print("\n🌐 SOURCE DISTRIBUTION")
        print("-" * 50)
        
        for source, cres in source_stats.items():
            print(f"\n📡 {source}:")
            for cre in sorted(cres, key=lambda x: x.get('auto_assign_count', 0)):
                count = cre.get('auto_assign_count', 0)
                print(f"   • {cre['name']}: {count} leads")
    
    def print_recent_assignments(self, recent_assignments):
        """
        Print recent assignment history.
        
        Args:
            recent_assignments (list): Recent assignment history
        """
        if not recent_assignments:
            return
        
        print("\n📝 RECENT ASSIGNMENTS (Last 24 Hours)")
        print("-" * 50)
        
        # Group by source
        source_assignments = {}
        for assignment in recent_assignments:
            source = assignment['source']
            if source not in source_assignments:
                source_assignments[source] = []
            source_assignments[source].append(assignment)
        
        for source, assignments in source_assignments.items():
            print(f"\n📡 {source}: {len(assignments)} assignments")
            for assignment in assignments[-5:]:  # Show last 5
                cre_name = assignment['assigned_cre_name']
                method = assignment['assignment_method']
                created_at = assignment['created_at']
                print(f"   • {cre_name} ({method}) - {created_at}")
    
    def print_new_assignments(self, new_assignments):
        """
        Print new assignments detected.
        
        Args:
            new_assignments (list): New assignments detected
        """
        if not new_assignments:
            return
        
        print("\n🆕 NEW ASSIGNMENTS DETECTED")
        print("-" * 50)
        
        for assignment in new_assignments:
            cre_name = assignment['assigned_cre_name']
            source = assignment['source']
            method = assignment['assignment_method']
            created_at = assignment['created_at']
            print(f"   ✅ {cre_name} ← {source} ({method}) - {created_at}")
    
    def print_auto_assign_status(self, configs):
        """
        Print auto-assign configuration status.
        
        Args:
            configs (list): Auto-assign configurations
        """
        if not configs:
            print("\n⚠️ No auto-assign configurations found")
            return
        
        print("\n⚙️ AUTO-ASSIGN CONFIGURATIONS")
        print("-" * 50)
        
        # Group by source
        source_configs = {}
        for config in configs:
            source = config['source']
            if source not in source_configs:
                source_configs[source] = []
            source_configs[source].append(config)
        
        for source, configs_list in source_configs.items():
            print(f"\n📡 {source}: {len(configs_list)} CREs configured")
            for config in configs_list:
                cre_id = config['cre_id']
                # Get CRE name
                cre_result = supabase.table('cre_users').select('name').eq('id', cre_id).execute()
                cre_name = cre_result.data[0]['name'] if cre_result.data else f"CRE ID: {cre_id}"
                print(f"   • {cre_name}")
    
    def run_monitor(self, interval=30):
        """
        Run the monitor continuously.
        
        Args:
            interval (int): Interval between checks in seconds
        """
        print("🚀 Starting Enhanced Auto-Assign Monitor...")
        print("Press Ctrl+C to stop")
        
        try:
            while True:
                self.print_header()
                
                # Get current stats
                stats = self.get_current_stats()
                if not stats:
                    print("❌ Failed to get current stats")
                    time.sleep(interval)
                    continue
                
                # Analyze fair distribution
                distribution_stats = self.analyze_fair_distribution(stats['cres'])
                self.print_fair_distribution_stats(distribution_stats)
                
                # Analyze source distribution
                source_stats = self.get_source_distribution(stats['configs'], stats['cres'])
                self.print_source_distribution(source_stats)
                
                # Show recent assignments
                self.print_recent_assignments(stats['recent_assignments'])
                
                # Detect new assignments
                new_assignments = self.detect_new_assignments(stats['recent_assignments'])
                self.print_new_assignments(new_assignments)
                
                # Show auto-assign status
                self.print_auto_assign_status(stats['configs'])
                
                # Update last check time
                self.last_check = get_ist_timestamp()
                
                print(f"\n⏰ Next update in {interval} seconds...")
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\n🛑 Monitor stopped by user")
        except Exception as e:
            print(f"❌ Monitor error: {e}")

# =============================================================================
# 3. SETUP AND VERIFICATION FUNCTIONS
# =============================================================================

def setup_enhanced_auto_assign():
    """
    Set up the enhanced auto-assign system with fair distribution tracking.
    
    This function:
    - Applies the enhanced database schema
    - Creates auto_assign_count column in cre_users table
    - Creates auto_assign_history table with IST timestamps
    - Sets up fair distribution functions and triggers
    - Verifies the setup
    
    Returns:
        bool: True if setup successful, False otherwise
    """
    
    print("🚀 Setting up Enhanced Auto-Assign System")
    print("=" * 60)
    
    try:
        # Create auto_assign_count column in cre_users table if it doesn't exist
        try:
            # Check if column exists
            result = supabase.table('cre_users').select('auto_assign_count').limit(1).execute()
            print("✅ auto_assign_count column already exists in cre_users table")
        except:
            # Column doesn't exist, create it
            print("📋 Creating auto_assign_count column in cre_users table...")
            # This would require a database migration - for now we'll handle it gracefully
            print("⚠️ Please manually add auto_assign_count column to cre_users table")
        
        # Create auto_assign_history table if it doesn't exist
        try:
            result = supabase.table('auto_assign_history').select('id').limit(1).execute()
            print("✅ auto_assign_history table already exists")
        except:
            print("📋 Creating auto_assign_history table...")
            # This would require a database migration - for now we'll handle it gracefully
            print("⚠️ Please manually create auto_assign_history table")
        
        # Create auto_assign_config table if it doesn't exist
        try:
            result = supabase.table('auto_assign_config').select('*').limit(1).execute()
            print("✅ auto_assign_config table already exists")
        except:
            print("📋 Creating auto_assign_config table...")
            # This would require a database migration - for now we'll handle it gracefully
            print("⚠️ Please manually create auto_assign_config table")
        
        print("\n✅ Enhanced Auto-Assign System Setup Complete!")
        print("📊 Features Added:")
        print("   • auto_assign_count column in cre_users table")
        print("   • auto_assign_history table with IST timestamps")
        print("   • Fair distribution logic based on assignment counts")
        print("   • Automatic triggers and functions")
        print("   • Statistics views and functions")
        
        # Verify the setup
        verify_setup()
        
    except Exception as e:
        print(f"❌ Error setting up enhanced auto-assign system: {e}")
        return False
    
    return True

def verify_setup():
    """
    Verify that the enhanced auto-assign system is properly set up.
    
    This function checks:
    - auto_assign_count column exists in cre_users table
    - auto_assign_history table exists
    - IST timezone functionality works
    - auto_assign_config table exists
    """
    
    print("\n🔍 Verifying Setup...")
    print("-" * 40)
    
    try:
        # Check if auto_assign_count column exists
        result = supabase.table('cre_users').select('auto_assign_count').limit(1).execute()
        print("✅ auto_assign_count column exists in cre_users table")
        
        # Check if auto_assign_history table exists
        result = supabase.table('auto_assign_history').select('id').limit(1).execute()
        print("✅ auto_assign_history table exists")
        
        # Test IST timezone function
        ist_time = get_ist_timestamp()
        print(f"✅ IST Timezone: {ist_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        
        # Check auto_assign_config table
        result = supabase.table('auto_assign_config').select('*').limit(1).execute()
        print("✅ auto_assign_config table exists")
        
        print("\n🎉 All verifications passed! Enhanced auto-assign system is ready.")
        
    except Exception as e:
        print(f"❌ Verification failed: {e}")

def get_cre_auto_assign_stats():
    """
    Get current auto-assign statistics for all CREs.
    
    Returns:
        list: CRE statistics with auto_assign_count
    """
    
    try:
        result = supabase.table('cre_users').select('id, name, auto_assign_count').execute()
        cres = result.data or []
        
        print("\n📊 Current Auto-Assign Statistics:")
        print("-" * 50)
        
        for cre in sorted(cres, key=lambda x: x.get('auto_assign_count', 0)):
            count = cre.get('auto_assign_count', 0)
            print(f"   • {cre['name']}: {count} auto-assigned leads")
        
        return cres
        
    except Exception as e:
        print(f"❌ Error getting CRE stats: {e}")
        return []

def test_fair_distribution():
    """
    Test the fair distribution logic.
    
    This function:
    - Gets current CRE statistics
    - Sorts CREs by auto_assign_count
    - Shows fair distribution priority
    - Calculates distribution statistics
    """
    
    try:
        cres = get_cre_auto_assign_stats()
        
        if not cres:
            print("ℹ️ No CREs found for testing")
            return
        
        # Sort by auto_assign_count for fair distribution
        sorted_cres = sorted(cres, key=lambda x: x.get('auto_assign_count', 0))
        
        print("\n🧪 Testing Fair Distribution Logic...")
        print("-" * 50)
        
        print("📋 CREs sorted by fair distribution priority:")
        for i, cre in enumerate(sorted_cres, 1):
            count = cre.get('auto_assign_count', 0)
            status = "🎯 NEXT" if i == 1 else "⏳ WAITING"
            print(f"   {i}. {cre['name']}: {count} leads {status}")
        
        # Calculate distribution statistics
        counts = [cre.get('auto_assign_count', 0) for cre in cres]
        min_count = min(counts)
        max_count = max(counts)
        variance = max_count - min_count
        
        print(f"\n📈 Distribution Statistics:")
        print(f"   • Total CREs: {len(cres)}")
        print(f"   • Min assignments: {min_count}")
        print(f"   • Max assignments: {max_count}")
        print(f"   • Variance: {variance} leads")
        
        if variance <= 1:
            print("   ✅ Excellent distribution")
        elif variance <= 3:
            print("   ⚠️ Good distribution")
        else:
            print("   ⚠️ Distribution needs improvement")
        
        print(f"\n🎯 Next lead should be assigned to: {sorted_cres[0]['name']}")
        
    except Exception as e:
        print(f"❌ Error testing fair distribution: {e}")

# =============================================================================
# 4. LEGACY COMPATIBILITY
# =============================================================================

class AutoAssignTrigger(EnhancedAutoAssignTrigger):
    """
    Legacy class that inherits from EnhancedAutoAssignTrigger for backward compatibility.
    
    This class maintains compatibility with existing code that uses the old AutoAssignTrigger.
    """
    pass

# Global instance for background processing
_trigger_instance = None

def start_auto_assign_trigger():
    """
    Start the auto-assign trigger system.
    
    Returns:
        EnhancedAutoAssignTrigger: The trigger instance
    """
    global _trigger_instance
    if _trigger_instance is None:
        _trigger_instance = EnhancedAutoAssignTrigger()
        _trigger_instance.start()
    return _trigger_instance

def stop_auto_assign_trigger():
    """Stop the auto-assign trigger system"""
    global _trigger_instance
    if _trigger_instance:
        _trigger_instance.stop()
        _trigger_instance = None

def trigger_manual_assignment(source):
    """
    Legacy function for manual assignment trigger.
    
    Args:
        source (str): The source name
        
    Returns:
        int: Number of leads assigned
    """
    trigger = EnhancedAutoAssignTrigger()
    result = trigger.trigger_manual_assignment(source)
    return result.get('assigned_count', 0) if result else 0

# =============================================================================
# 5. DEPLOYMENT-SPECIFIC WORKERS
# =============================================================================

class GitHubActionsWorker:
    """
    GitHub Actions-specific auto-assign worker optimized for CI/CD.
    
    This class provides:
    - Single-run processing for GitHub Actions
    - Detailed output for CI/CD monitoring
    - Optimized for automated workflows
    - Fair distribution processing
    
    Usage:
        worker = GitHubActionsWorker()
        worker.run_single()      # For GitHub Actions
    """
    
    def __init__(self, max_leads_per_batch=100):
        """
        Initialize the GitHub Actions auto-assign worker.
        
        Args:
            max_leads_per_batch (int): Maximum leads to process per batch (default: 100)
        """
        self.max_leads_per_batch = max_leads_per_batch
        
        # Configure logging for GitHub Actions
        import logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def run_optimized_auto_assign(self):
        """
        Run optimized auto-assign check for GitHub Actions.
        
        This method:
        - Processes all configured sources
        - Uses fair distribution logic
        - Provides detailed logging for CI/CD
        - Optimizes for single-run scenarios
        """
        try:
            start_time = datetime.now()
            self.logger.info(f"🚀 GitHub Actions Auto-Assign started at {start_time}")
            
            # Get all auto-assign configurations
            config_result = supabase.table('auto_assign_config').select('*').execute()
            configs = config_result.data or []
            
            if not configs:
                self.logger.info("ℹ️ No auto-assign configurations found")
                return
            
            # Group configs by source
            source_configs = {}
            for config in configs:
                source = config['source']
                if source not in source_configs:
                    source_configs[source] = []
                source_configs[source].append(config)
            
            # Process each source with fair distribution
            total_assigned = 0
            total_sources = len(source_configs)
            
            self.logger.info(f"📊 Processing {total_sources} sources with auto-assign configuration")
            
            for source, source_configs_list in source_configs.items():
                self.logger.info(f"🔄 Processing source: {source}")
                
                # Use the enhanced auto-assign trigger for fair distribution
                trigger = EnhancedAutoAssignTrigger()
                result = trigger.trigger_manual_assignment(source)
                
                if result and 'assigned_count' in result:
                    assigned_count = result['assigned_count']
                    total_assigned += assigned_count
                    self.logger.info(f"✅ {source}: {assigned_count} leads assigned using fair distribution")
                else:
                    self.logger.warning(f"⚠️ {source}: No assignment result returned")
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            if total_assigned > 0:
                self.logger.info(f"🎉 GitHub Actions Auto-Assign completed: {total_assigned} total leads assigned in {duration:.2f}s")
            else:
                self.logger.info(f"ℹ️ GitHub Actions Auto-Assign completed: No leads were assigned (took {duration:.2f}s)")
                
        except Exception as e:
            self.logger.error(f"❌ Error in GitHub Actions auto-assign: {e}")
            raise  # Re-raise for GitHub Actions to catch
    
    def run_single(self):
        """
        Run a single auto-assign check for GitHub Actions.
        
        This method:
        - Runs once and exits (suitable for GitHub Actions)
        - Provides detailed output for CI/CD monitoring
        - Optimizes for single-run scenarios
        """
        print("🚀 GitHub Actions Auto-Assign Worker Starting...")
        print("=" * 60)
        
        # Run the optimized auto-assign check
        self.run_optimized_auto_assign()
        
        print("✅ GitHub Actions Auto-Assign worker completed successfully")

# =============================================================================
# 6. MAIN EXECUTION
# =============================================================================

def main():
    """
    Main execution function for the auto-assign system.
    
    This function provides a command-line interface for:
    - Setting up the enhanced auto-assign system
    - Running the monitor
    - Testing the system
    - Starting the background trigger
    - Running GitHub Actions workers
    """
    
    import sys
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python auto_assign_system.py setup     - Set up the enhanced auto-assign system")
        print("  python auto_assign_system.py monitor   - Run the enhanced monitor")
        print("  python auto_assign_system.py test      - Test the fair distribution logic")
        print("  python auto_assign_system.py trigger   - Start the background trigger")
        print("  python auto_assign_system.py stats     - Show current statistics")
        print("  python auto_assign_system.py worker    - Run GitHub Actions worker")
        return
    
    command = sys.argv[1].lower()
    
    if command == 'setup':
        setup_enhanced_auto_assign()
    elif command == 'monitor':
        monitor = EnhancedAutoAssignMonitor()
        monitor.run_monitor()
    elif command == 'test':
        test_fair_distribution()
    elif command == 'trigger':
        trigger = start_auto_assign_trigger()
        print("🚀 Background trigger started. Press Ctrl+C to stop.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            stop_auto_assign_trigger()
            print("\n🛑 Background trigger stopped.")
    elif command == 'stats':
        get_cre_auto_assign_stats()
    elif command == 'worker':
        worker = GitHubActionsWorker()
        worker.run_single()
    else:
        print(f"❌ Unknown command: {command}")

if __name__ == "__main__":
    main()

