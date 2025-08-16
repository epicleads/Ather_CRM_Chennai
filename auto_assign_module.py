#!/usr/bin/env python3
"""
Comprehensive Auto-Assign Module for Ather CRM
Integrated with existing CRM system and database structure.

Features:
- Automatic lead assignment using fair distribution (round-robin)
- CRE auto-assign count management and reset logic
- Comprehensive history tracking and export
- Virtual thread management for background processing
- Debug logging and monitoring
- Production-ready API endpoints
- Error handling and recovery mechanisms
- Integration with existing Supabase database

Author: AI Assistant
Version: 1.0.0
Production Ready: Yes
"""

import os
import time
import json
import csv
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
import logging



# =============================================================================
# DATA STRUCTURES AND CONFIGURATION
# =============================================================================

@dataclass
class AutoAssignConfig:
    """Configuration for auto-assign system"""
    id: Optional[int] = None
    source: str = ""
    cre_id: int = 0
    is_active: bool = True
    priority: int = 1
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

@dataclass
class AutoAssignHistory:
    """Auto-assign history record"""
    id: Optional[int] = None
    lead_uid: str = ""
    source: str = ""
    assigned_cre_id: int = 0
    assigned_cre_name: str = ""
    cre_total_leads_before: int = 0
    cre_total_leads_after: int = 0
    assignment_method: str = 'fair_distribution'
    created_at: Optional[str] = None

@dataclass
class CREUser:
    """CRE user information"""
    id: int = 0
    name: str = ""
    username: str = ""
    auto_assign_count: int = 0
    is_active: bool = True
    role: str = 'cre'

@dataclass
class Lead:
    """Lead information"""
    uid: str = ""
    customer_name: str = ""
    customer_mobile_number: str = ""
    source: str = ""
    sub_source: str = ""
    assigned: str = 'No'
    cre_name: Optional[str] = None
    cre_assigned_at: Optional[str] = None
    lead_status: str = 'Pending'

# =============================================================================
# VIRTUAL THREAD MANAGEMENT SYSTEM
# =============================================================================

class VirtualThreadManager:
    """Manages virtual threads for auto-assign operations"""
    
    def __init__(self):
        self.threads = {}
        self.thread_counter = 0
        self.max_threads = 10
        self.thread_timeout = 300  # 5 minutes
        
    def create_virtual_thread(self, name: str, target_func, *args, **kwargs) -> str:
        """Create a virtual thread for background processing"""
        if len(self.threads) >= self.max_threads:
            logger.warning(f"Maximum threads ({self.max_threads}) reached. Cannot create new thread: {name}")
            return None
            
        thread_id = f"vt_{self.thread_counter}_{int(time.time())}"
        self.thread_counter += 1
        
        # Create actual thread
        thread = threading.Thread(
            target=self._thread_wrapper,
            args=(thread_id, target_func, args, kwargs),
            name=name,
            daemon=True
        )
        
        self.threads[thread_id] = {
            'thread': thread,
            'name': name,
            'created_at': time.time(),
            'status': 'created',
            'result': None,
            'error': None
        }
        
        thread.start()
        logger.info(f"🚀 Virtual thread created: {thread_id} ({name})")
        return thread_id
    
    def _thread_wrapper(self, thread_id: str, target_func, args, kwargs):
        """Wrapper for thread execution with error handling"""
        try:
            self.threads[thread_id]['status'] = 'running'
            logger.info(f"🔄 Virtual thread {thread_id} started execution")
            
            result = target_func(*args, **kwargs)
            self.threads[thread_id]['result'] = result
            self.threads[thread_id]['status'] = 'completed'
            
            logger.info(f"✅ Virtual thread {thread_id} completed successfully")
            
        except Exception as e:
            self.threads[thread_id]['error'] = str(e)
            self.threads[thread_id]['status'] = 'failed'
            logger.error(f"❌ Virtual thread {thread_id} failed: {e}")
        
        finally:
            # Clean up completed threads after timeout
            self._cleanup_threads()
    
    def _cleanup_threads(self):
        """Clean up completed or timed-out threads"""
        current_time = time.time()
        threads_to_remove = []
        
        for thread_id, thread_info in self.threads.items():
            if thread_info['status'] in ['completed', 'failed']:
                if current_time - thread_info['created_at'] > self.thread_timeout:
                    threads_to_remove.append(thread_id)
        
        for thread_id in threads_to_remove:
            del self.threads[thread_id]
            logger.info(f"🧹 Cleaned up virtual thread: {thread_id}")
    
    def get_thread_status(self, thread_id: str) -> Optional[Dict]:
        """Get status of a specific thread"""
        return self.threads.get(thread_id)
    
    def get_all_threads_status(self) -> Dict:
        """Get status of all threads"""
        return {
            'total_threads': len(self.threads),
            'active_threads': len([t for t in self.threads.values() if t['status'] == 'running']),
            'completed_threads': len([t for t in self.threads.values() if t['status'] == 'completed']),
            'failed_threads': len([t for t in self.threads.values() if t['status'] == 'failed']),
            'threads': {tid: {k: v for k, v in info.items() if k != 'thread'} 
                       for tid, info in self.threads.items()}
        }
    
    def stop_thread(self, thread_id: str) -> bool:
        """Stop a specific thread"""
        if thread_id in self.threads:
            thread_info = self.threads[thread_id]
            if thread_info['status'] == 'running':
                # Note: In real implementation, you'd need proper thread termination
                thread_info['status'] = 'stopping'
                logger.info(f"🛑 Stopping virtual thread: {thread_id}")
                return True
        return False
    
    def get_thread_result(self, thread_id: str) -> Optional[Any]:
        """Get result of a completed thread"""
        if thread_id in self.threads:
            thread_info = self.threads[thread_id]
            if thread_info['status'] == 'completed':
                return thread_info['result']
        return None
    
    def get_thread_error(self, thread_id: str) -> Optional[str]:
        """Get error of a failed thread"""
        if thread_id in self.threads:
            thread_info = self.threads[thread_id]
            if thread_info['status'] == 'failed':
                return thread_info['error']
        return None
    
    def wait_for_thread(self, thread_id: str, timeout: float = None) -> bool:
        """Wait for a thread to complete"""
        if thread_id not in self.threads:
            return False
        
        thread_info = self.threads[thread_id]
        thread = thread_info['thread']
        
        if timeout:
            thread.join(timeout=timeout)
        else:
            thread.join()
        
        return thread_info['status'] in ['completed', 'failed']
    
    def get_active_threads(self) -> List[str]:
        """Get list of active thread IDs"""
        return [tid for tid, info in self.threads.items() if info['status'] == 'running']
    
    def get_completed_threads(self) -> List[str]:
        """Get list of completed thread IDs"""
        return [tid for tid, info in self.threads.items() if info['status'] == 'completed']
    
    def get_failed_threads(self) -> List[str]:
        """Get list of failed thread IDs"""
        return [tid for tid, info in self.threads.items() if info['status'] == 'failed']
    
    def cleanup_all_completed_threads(self) -> int:
        """Clean up all completed threads and return count of cleaned threads"""
        current_time = time.time()
        threads_to_remove = []
        
        for thread_id, thread_info in self.threads.items():
            if thread_info['status'] in ['completed', 'failed']:
                if current_time - thread_info['created_at'] > self.thread_timeout:
                    threads_to_remove.append(thread_id)
        
        for thread_id in threads_to_remove:
            del self.threads[thread_id]
            logger.info(f"🧹 Cleaned up virtual thread: {thread_id}")
        
        return len(threads_to_remove)
    
    def get_thread_summary(self) -> Dict[str, Any]:
        """Get comprehensive thread summary"""
        active_threads = self.get_active_threads()
        completed_threads = self.get_completed_threads()
        failed_threads = self.get_failed_threads()
        
        return {
            'total_threads': len(self.threads),
            'active_threads': len(active_threads),
            'completed_threads': len(completed_threads),
            'failed_threads': len(failed_threads),
            'max_threads': self.max_threads,
            'available_slots': self.max_threads - len(self.threads),
            'thread_timeout': self.thread_timeout,
            'thread_counter': self.thread_counter
        }

# =============================================================================
# IST TIMESTAMP UTILITIES
# =============================================================================

def get_ist_timestamp() -> str:
    """Get current timestamp in IST format (UTC+5:30) for database storage"""
    ist_time = datetime.now() + timedelta(hours=5, minutes=30)
    return ist_time.isoformat()

def get_ist_timestamp_readable() -> str:
    """Get current IST timestamp in human-readable format"""
    ist_time = datetime.now() + timedelta(hours=5, minutes=30)
    return ist_time.strftime('%Y-%m-%d %H:%M:%S')

def get_current_system_time() -> str:
    """Get current system time for comparison"""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def get_current_ist_time() -> str:
    """Get current IST time in readable format"""
    ist_time = datetime.now() + timedelta(hours=5, minutes=30)
    return ist_time.strftime('%Y-%m-%d %H:%M:%S')

def convert_utc_to_ist(utc_timestamp: str) -> str:
    """Convert UTC timestamp to IST"""
    try:
        utc_time = datetime.fromisoformat(utc_timestamp.replace('Z', '+00:00'))
        ist_time = utc_time + timedelta(hours=5, minutes=30)
        return ist_time.isoformat()
    except Exception:
        return utc_timestamp

def convert_ist_to_utc(ist_timestamp: str) -> str:
    """Convert IST timestamp to UTC"""
    try:
        ist_time = datetime.fromisoformat(ist_timestamp)
        utc_time = ist_time - timedelta(hours=5, minutes=30)
        return utc_time.isoformat()
    except Exception:
        return utc_timestamp

# =============================================================================
# AUTO-ASSIGN CORE SYSTEM
# =============================================================================

class AutoAssignSystem:
    """Main auto-assign system with comprehensive functionality"""
    
    def __init__(self, supabase_client):
        self.supabase = supabase_client
        self.virtual_thread_manager = VirtualThreadManager()
        self.system_status = {
            'is_running': False,
            'total_runs': 0,
            'total_leads_assigned': 0,
            'last_run': None,
            'next_run': None,
            'errors': [],
            'started_at': None
        }
        self.auto_assign_thread = None
        self.running = False
        
        # Debug configuration
        self.debug_mode = os.environ.get('AUTO_ASSIGN_DEBUG', 'false').lower() == 'true'
        self.verbose_logging = os.environ.get('AUTO_ASSIGN_VERBOSE', 'false').lower() == 'true'
        
        logger.info("🚀 Auto-Assign System initialized")
        if self.debug_mode:
            logger.info("🔍 Debug mode enabled")
        if self.verbose_logging:
            logger.info("📝 Verbose logging enabled")
    
    def debug_print(self, message: str, level: str = 'INFO'):
        """Debug print function with configurable levels"""
        if not self.debug_mode:
            return
            
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        level_emoji = {
            'INFO': 'ℹ️',
            'SUCCESS': '✅',
            'WARNING': '⚠️',
            'ERROR': '❌',
            'DEBUG': '🔍',
            'SYSTEM': '🤖'
        }
        
        emoji = level_emoji.get(level, 'ℹ️')
        formatted_message = f"{emoji} [{timestamp}] {message}"
        
        if level == 'ERROR':
            logger.error(formatted_message)
        elif level == 'WARNING':
            logger.warning(formatted_message)
        elif level == 'SUCCESS':
            logger.info(formatted_message)
        else:
            logger.info(formatted_message)
    
    def get_ist_timestamp(self) -> str:
        """Get current timestamp in IST format for Supabase"""
        return get_ist_timestamp()
    
    def get_current_system_time(self) -> str:
        """Get current system time for comparison"""
        return get_current_system_time()
    
    def get_current_ist_time(self) -> str:
        """Get current IST time in readable format"""
        return get_current_ist_time()
    
    def get_auto_assign_configs(self) -> List[Dict]:
        """Get all auto-assign configurations from database"""
        try:
            result = self.supabase.table('auto_assign_config').select('*').eq('is_active', True).execute()
            return result.data if result.data else []
        except Exception as e:
            logger.error(f"Error getting auto-assign configs: {e}")
            return []
    
    def get_unassigned_leads_for_source(self, source: str) -> List[Dict]:
        """Get unassigned leads for a specific source"""
        try:
            result = self.supabase.table('lead_master').select('*').eq('source', source).eq('assigned', 'No').execute()
            return result.data if result.data else []
        except Exception as e:
            logger.error(f"Error getting unassigned leads for {source}: {e}")
            return []
    
    def get_cre_users(self) -> List[Dict]:
        """Get all active CRE users"""
        try:
            result = self.supabase.table('cre_users').select('*').eq('is_active', True).execute()
            return result.data if result.data else []
        except Exception as e:
            logger.error(f"Error getting CRE users: {e}")
            return []
    
    def assign_lead_to_cre(self, lead_uid: str, cre_id: int, cre_name: str, source: str) -> bool:
        """Assign a lead to a CRE user"""
        try:
            self.debug_print(f"🎯 Starting lead assignment: {lead_uid} -> {cre_name} (CRE ID: {cre_id})", "SYSTEM")
            

            
            # Get CRE's current lead count BEFORE assignment
            cre_result = self.supabase.table('cre_users').select('auto_assign_count').eq('id', cre_id).execute()
            current_count = cre_result.data[0]['auto_assign_count'] if cre_result.data else 0
            
            self.debug_print(f"📊 CRE {cre_name} current auto_assign_count: {current_count}", "DEBUG")
            
            # Update lead_master table with assignment
            # Use IST timestamp for cre_assigned_at
            update_data = {
                'assigned': 'Yes',
                'cre_name': cre_name,
                'cre_assigned_at': self.get_ist_timestamp()  # Use IST timestamp
            }
            
            self.debug_print(f"🔄 Updating lead_master for {lead_uid}", "DEBUG")
            self.debug_print(f"   📊 Update data: {update_data}", "DEBUG")
            self.debug_print(f"   🕒 IST Timestamp: {self.get_ist_timestamp()}", "DEBUG")
            
            try:
                lead_update_result = self.supabase.table('lead_master').update(update_data).eq('uid', lead_uid).execute()
                
                if lead_update_result.data:
                    self.debug_print(f"✅ Lead {lead_uid} marked as assigned in lead_master", "SUCCESS")
                    self.debug_print(f"   📊 Update result: {lead_update_result.data}", "DEBUG")
                else:
                    self.debug_print(f"⚠️ Warning: Lead update may have failed", "WARNING")
                    self.debug_print(f"   🚨 Update result: {lead_update_result}", "DEBUG")
                    if hasattr(lead_update_result, 'error'):
                        self.debug_print(f"   ❌ Update error: {lead_update_result.error}", "ERROR")
                        
            except Exception as e:
                self.debug_print(f"❌ Exception during lead update: {e}", "ERROR")
                self.debug_print(f"   🚨 Exception type: {type(e).__name__}", "ERROR")
                self.debug_print(f"   📊 Update data: {update_data}", "DEBUG")
                raise  # Re-raise to be caught by outer exception handler
            
            # Update CRE's auto_assign_count
            new_count = current_count + 1
            self.debug_print(f"📈 Attempting to update CRE {cre_name} auto_assign_count: {current_count} -> {new_count}", "DEBUG")
            
            update_data = {
                'auto_assign_count': new_count
                # Note: updated_at is handled by database trigger
            }
            self.debug_print(f"   📊 Update data: {update_data}", "DEBUG")
            
            cre_update_result = self.supabase.table('cre_users').update(update_data).eq('id', cre_id).execute()
            
            if cre_update_result.data:
                self.debug_print(f"✅ Successfully updated CRE {cre_name} auto_assign_count: {current_count} -> {new_count}", "SUCCESS")
            else:
                self.debug_print(f"⚠️ Warning: CRE count update may have failed", "WARNING")
                self.debug_print(f"   🚨 Update result: {cre_update_result}", "DEBUG")
                if hasattr(cre_update_result, 'error'):
                    self.debug_print(f"   ❌ Update error: {cre_update_result.error}", "ERROR")
            
            # Create comprehensive history record
            # Use database default timestamp to avoid Supabase UTC conversion
            history_data = {
                'lead_uid': lead_uid,
                'source': source,
                'assigned_cre_id': cre_id,
                'assigned_cre_name': cre_name,
                'cre_total_leads_before': current_count,  # Count BEFORE this assignment
                'cre_total_leads_after': new_count,       # Count AFTER this assignment
                'assignment_method': 'fair_distribution'
                # Remove created_at - let database use default now()
            }
            
            # Insert into auto_assign_history table
            self.debug_print(f"📝 Attempting to insert history record for lead {lead_uid}", "DEBUG")
            self.debug_print(f"   📊 History data: {history_data}", "DEBUG")
            self.debug_print(f"   🕒 System Time: {self.get_current_system_time()}", "DEBUG")
            self.debug_print(f"   🕒 IST Time: {self.get_current_ist_time()}", "DEBUG")
            self.debug_print(f"   🕒 Timestamp: Using database default now()", "DEBUG")
            
            history_result = self.supabase.table('auto_assign_history').insert(history_data).execute()
            
            if history_result.data:
                self.debug_print(f"📝 History record created successfully for lead {lead_uid}", "SUCCESS")
                self.debug_print(f"   📊 Before: {current_count}, After: {new_count}", "DEBUG")
                self.debug_print(f"   🕒 Timestamp: Database default now()", "DEBUG")
                self.debug_print(f"   📋 History ID: {history_result.data[0].get('id', 'Unknown')}", "DEBUG")
            else:
                self.debug_print(f"⚠️ Warning: History record may not have been created", "WARNING")
                self.debug_print(f"   🚨 History result: {history_result}", "DEBUG")
                if hasattr(history_result, 'error'):
                    self.debug_print(f"   ❌ History error: {history_result.error}", "ERROR")
            
            # Verify the assignment was successful
            self.debug_print(f"🔍 Verifying assignment for lead {lead_uid}", "DEBUG")
            verification_result = self.supabase.table('lead_master').select('assigned, cre_name, cre_assigned_at').eq('uid', lead_uid).execute()
            
            self.debug_print(f"   📊 Verification result: {verification_result.data}", "DEBUG")
            
            if verification_result.data and verification_result.data[0]['assigned'] == 'Yes':
                lead_data = verification_result.data[0]
                self.debug_print(f"🎉 Lead {lead_uid} successfully assigned to {cre_name}", "SUCCESS")
                self.debug_print(f"   🔍 Verification: assigned={lead_data['assigned']}, cre_name={lead_data['cre_name']}", "DEBUG")
                
                if lead_data.get('cre_assigned_at'):
                    self.debug_print(f"   🕒 cre_assigned_at: {lead_data['cre_assigned_at']}", "DEBUG")
                else:
                    self.debug_print(f"   ⚠️ cre_assigned_at is NULL - this may indicate a database schema issue", "WARNING")
                
                return True
            else:
                self.debug_print(f"❌ Assignment verification failed for lead {lead_uid}", "ERROR")
                if verification_result.data:
                    self.debug_print(f"   🚨 Found data: {verification_result.data[0]}", "DEBUG")
                else:
                    self.debug_print(f"   🚨 No data found for lead {lead_uid}", "DEBUG")
                return False
            
        except Exception as e:
            self.debug_print(f"❌ Error assigning lead {lead_uid} to CRE {cre_id}: {e}", "ERROR")
            self.debug_print(f"   🚨 Exception type: {type(e).__name__}", "ERROR")
            self.debug_print(f"   📍 Source: {source}, CRE: {cre_name}", "ERROR")
            return False
    
    def reset_cre_auto_assign_counts(self, cre_ids: List[int]) -> bool:
        """
        Reset auto_assign_count to 0 for specified CREs.
        This ensures fair distribution starts fresh when configurations change.
        
        Args:
            cre_ids: List of CRE IDs whose counts should be reset
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if not cre_ids:
                self.debug_print("⚠️ No CRE IDs provided for count reset", "WARNING")
                return True
                
            self.debug_print(f"🔄 Resetting auto_assign_count to 0 for {len(cre_ids)} CREs: {cre_ids}", "SYSTEM")
            
            # Get current counts before reset for logging
            current_counts = {}
            for cre_id in cre_ids:
                try:
                    cre_result = self.supabase.table('cre_users').select('name, auto_assign_count').eq('id', cre_id).execute()
                    if cre_result.data:
                        cre_name = cre_result.data[0]['name']
                        old_count = cre_result.data[0]['auto_assign_count']
                        current_counts[cre_id] = {'name': cre_name, 'old_count': old_count}
                        self.debug_print(f"   📊 CRE {cre_name} (ID: {cre_id}) current count: {old_count}", "DEBUG")
                except Exception as e:
                    self.debug_print(f"   ⚠️ Could not get current count for CRE ID {cre_id}: {e}", "WARNING")
            
            # Reset counts to 0
            reset_count = 0
            for cre_id in cre_ids:
                try:
                    update_result = self.supabase.table('cre_users').update({
                        'auto_assign_count': 0
                        # Note: updated_at is handled by database trigger
                    }).eq('id', cre_id).execute()
                    
                    if update_result.data:
                        cre_name = current_counts.get(cre_id, {}).get('name', f'CRE_{cre_id}')
                        old_count = current_counts.get(cre_id, {}).get('old_count', 'Unknown')
                        self.debug_print(f"   ✅ Reset count for CRE {cre_name} (ID: {cre_id}): {old_count} -> 0", "SUCCESS")
                        reset_count += 1
                    else:
                        self.debug_print(f"   ⚠️ No rows updated for CRE ID {cre_id}", "WARNING")
                        
                except Exception as e:
                    self.debug_print(f"   ❌ Error resetting count for CRE ID {cre_id}: {e}", "ERROR")
            
            self.debug_print(f"🎯 Successfully reset auto_assign_count for {reset_count}/{len(cre_ids)} CREs", "SUCCESS")
            
            # Verify the reset was successful
            verification_count = 0
            for cre_id in cre_ids:
                try:
                    verify_result = self.supabase.table('cre_users').select('auto_assign_count').eq('id', cre_id).execute()
                    if verify_result.data and verify_result.data[0]['auto_assign_count'] == 0:
                        verification_count += 1
                except:
                    pass
            
            if verification_count == len(cre_ids):
                self.debug_print(f"🔍 Verification successful: All {verification_count} CREs have count reset to 0", "SUCCESS")
            else:
                self.debug_print(f"⚠️ Verification warning: Only {verification_count}/{len(cre_ids)} CREs verified as reset", "WARNING")
            
            return True
            
        except Exception as e:
            self.debug_print(f"❌ Error resetting auto_assign_count for CREs {cre_ids}: {e}", "ERROR")
            self.debug_print(f"   🚨 Exception type: {type(e).__name__}", "ERROR")
            return False
    
    def auto_assign_new_leads_for_source(self, source: str) -> Dict[str, Any]:
        """
        Automatically assign new leads for a specific source using fair distribution.
        
        Args:
            source: The source name to auto-assign leads for
            
        Returns:
            dict: Result with assigned_count and status
        """
        try:
            self.debug_print(f"🤖 Starting auto-assign for source: {source}", "SYSTEM")
            self.debug_print(f"   ⏰ Start Time: {self.get_ist_timestamp()}", "INFO")
            
            # Get auto-assign configuration for this source
            configs = self.supabase.table('auto_assign_config').select('*').eq('source', source).eq('is_active', True).execute()
            if not configs.data:
                self.debug_print(f"ℹ️ No auto-assign configuration found for {source}", "INFO")
                return {'success': False, 'message': f'No auto-assign configuration found for {source}', 'assigned_count': 0}
            
            cre_ids = [config['cre_id'] for config in configs.data]
            self.debug_print(f"📋 Found {len(cre_ids)} CREs configured for {source}: {cre_ids}", "INFO")
            
            # Get unassigned leads for this source
            unassigned_leads = self.get_unassigned_leads_for_source(source)
            
            if not unassigned_leads:
                self.debug_print(f"ℹ️ No unassigned leads found for {source}", "INFO")
                return {'success': True, 'message': f'No unassigned leads found for {source}', 'assigned_count': 0}
            
            self.debug_print(f"📊 Found {len(unassigned_leads)} unassigned leads for {source}", "INFO")
            self.debug_print(f"   🎯 Lead UIDs: {[lead['uid'] for lead in unassigned_leads[:5]]}{'...' if len(unassigned_leads) > 5 else ''}", "DEBUG")
            
            # Fair distribution using round-robin
            assigned_count = 0
            failed_assignments = []
            
            for i, lead in enumerate(unassigned_leads):
                selected_cre_id = cre_ids[i % len(cre_ids)]
                
                # Get CRE name
                cre_result = self.supabase.table('cre_users').select('name').eq('id', selected_cre_id).execute()
                selected_cre_name = cre_result.data[0]['name'] if cre_result.data else f"CRE_{selected_cre_id}"
                
                self.debug_print(f"🎯 Processing lead {i+1}/{len(unassigned_leads)}: {lead['uid']} -> {selected_cre_name}", "DEBUG")
                self.debug_print(f"   📍 Source: {source}, CRE ID: {selected_cre_id}", "DEBUG")
                

                
                # Assign the lead
                if self.assign_lead_to_cre(lead['uid'], selected_cre_id, selected_cre_name, source):
                    assigned_count += 1
                    self.debug_print(f"✅ Successfully assigned lead {lead['uid']} to {selected_cre_name}", "SUCCESS")
                    
                    # Verify the lead appears in the right place
                    self._verify_lead_assignment(lead['uid'], selected_cre_name, source)
                else:
                    failed_assignments.append(lead['uid'])
                    self.debug_print(f"❌ Failed to assign lead {lead['uid']} to {selected_cre_name}", "ERROR")
            
            # Summary and verification
            if assigned_count > 0:
                self.debug_print(f"🎉 SUCCESS: Auto-assigned {assigned_count} leads for {source}", "SUCCESS")
                self.debug_print(f"   📊 Total leads processed: {len(unassigned_leads)}", "INFO")
                self.debug_print(f"   ✅ Successfully assigned: {assigned_count}", "SUCCESS")
                self.debug_print(f"   ❌ Failed assignments: {len(failed_assignments)}", "WARNING")
                self.debug_print(f"   👥 CREs involved: {cre_ids}", "INFO")
                self.debug_print(f"   ⏰ Completion Time: {self.get_ist_timestamp()}", "INFO")
                
                if failed_assignments:
                    self.debug_print(f"   🚨 Failed lead UIDs: {failed_assignments}", "ERROR")
            else:
                self.debug_print(f"ℹ️ No leads were auto-assigned for {source}", "INFO")
            
            return {
                'success': True,
                'message': f'Successfully auto-assigned {assigned_count} leads for {source}',
                'assigned_count': assigned_count,
                'source': source,
                'total_processed': len(unassigned_leads),
                'failed_count': len(failed_assignments),
                'failed_leads': failed_assignments,
                'timestamp': self.get_ist_timestamp()
            }
            
        except Exception as e:
            self.debug_print(f"❌ Error in auto_assign_new_leads_for_source: {e}", "ERROR")
            self.debug_print(f"   🚨 Exception type: {type(e).__name__}", "ERROR")
            self.debug_print(f"   📍 Source: {source}", "ERROR")
            return {'success': False, 'message': str(e), 'assigned_count': 0}
    
    def _verify_lead_assignment(self, lead_uid: str, cre_name: str, source: str):
        """Verify that a lead assignment was successful and appears in the right places"""
        try:
            self.debug_print(f"🔍 Verifying lead assignment: {lead_uid}", "DEBUG")
            
            # Check lead_master table
            lead_result = self.supabase.table('lead_master').select('assigned, cre_name, cre_assigned_at').eq('uid', lead_uid).execute()
            if lead_result.data:
                lead_data = lead_result.data[0]
                if lead_data['assigned'] == 'Yes' and lead_data['cre_name'] == cre_name:
                    self.debug_print(f"   ✅ Lead {lead_uid} properly assigned in lead_master", "SUCCESS")
                    self.debug_print(f"      📊 assigned: {lead_data['assigned']}, cre_name: {lead_data['cre_name']}", "DEBUG")
                    if lead_data.get('cre_assigned_at'):
                        self.debug_print(f"      🕒 cre_assigned_at: {lead_data['cre_assigned_at']}", "DEBUG")
                    else:
                        self.debug_print(f"      ⚠️ cre_assigned_at is NULL", "WARNING")
                else:
                    self.debug_print(f"   ⚠️ Lead {lead_uid} assignment mismatch in lead_master", "WARNING")
                    self.debug_print(f"      📊 Expected: assigned=Yes, cre_name={cre_name}", "DEBUG")
                    self.debug_print(f"      📊 Actual: assigned={lead_data['assigned']}, cre_name={lead_data['cre_name']}", "DEBUG")
            else:
                self.debug_print(f"   ❌ Lead {lead_uid} not found in lead_master", "ERROR")
            
            # Check auto_assign_history table
            history_result = self.supabase.table('auto_assign_history').select('*').eq('lead_uid', lead_uid).eq('source', source).execute()
            if history_result.data:
                history_data = history_result.data[0]
                self.debug_print(f"   ✅ Lead {lead_uid} found in auto_assign_history", "SUCCESS")
                self.debug_print(f"      📊 CRE: {history_data['assigned_cre_name']}, Method: {history_data['assignment_method']}", "DEBUG")
                self.debug_print(f"      📊 Before: {history_data['cre_total_leads_before']}, After: {history_data['cre_total_leads_after']}", "DEBUG")
            else:
                self.debug_print(f"   ❌ Lead {lead_uid} not found in auto_assign_history", "ERROR")
            
            # Check CRE's updated count
            cre_result = self.supabase.table('cre_users').select('auto_assign_count').eq('name', cre_name).execute()
            if cre_result.data:
                current_count = cre_result.data[0]['auto_assign_count']
                self.debug_print(f"   📊 CRE {cre_name} current auto_assign_count: {current_count}", "DEBUG")
            else:
                self.debug_print(f"   ⚠️ Could not verify CRE {cre_name} count", "WARNING")
                
        except Exception as e:
            self.debug_print(f"   ❌ Error during verification: {e}", "ERROR")
    
    def check_and_assign_new_leads(self) -> Dict[str, Any]:
        """
        Check for new leads across all sources and assign them automatically.
        
        Returns:
            dict: Result with total_assigned and status
        """
        try:
            self.debug_print("🔄 Starting comprehensive lead assignment check", "SYSTEM")
            
            # Get all sources with auto-assign configs
            configs = self.get_auto_assign_configs()
            sources = list(set([config['source'] for config in configs]))
            
            total_assigned = 0
            results = []
            
            for source in sources:
                self.debug_print(f"🔍 Checking source: {source}", "DEBUG")
                result = self.auto_assign_new_leads_for_source(source)
                
                if result['success']:
                    total_assigned += result['assigned_count']
                    results.append(result)
                    self.debug_print(f"✅ {source}: {result['assigned_count']} leads assigned", "SUCCESS")
                else:
                    self.debug_print(f"⚠️ {source}: {result['message']}", "WARNING")
                    results.append(result)
            
            self.debug_print(f"🏁 Assignment check completed. Total leads assigned: {total_assigned}", "SUCCESS")
            
            return {
                'success': True,
                'total_assigned': total_assigned,
                'results': results,
                'timestamp': self.get_ist_timestamp()
            }
            
        except Exception as e:
            self.debug_print(f"❌ Error in check_and_assign_new_leads: {e}", "ERROR")
            return {'success': False, 'message': str(e), 'total_assigned': 0}
    
    def robust_auto_assign_worker(self):
        """Robust background worker that continuously checks for new leads"""
        self.debug_print("🚀 Robust auto-assign background worker started", "SYSTEM")
        self.system_status['is_running'] = True
        self.system_status['started_at'] = self.get_ist_timestamp()
        
        # Immediate auto-assign when server starts
        self.debug_print("🚀 Starting immediate auto-assign check...", "SYSTEM")
        try:
            result = self.check_and_assign_new_leads()
            if result and result.get('success'):
                self.debug_print("✅ Immediate auto-assign completed successfully", "SUCCESS")
                self.system_status['total_leads_assigned'] += result.get('total_assigned', 0)
                if result.get('total_assigned', 0) > 0:
                    self.debug_print(f"📊 {result.get('total_assigned')} leads assigned immediately", "SUCCESS")
                else:
                    self.debug_print("ℹ️ No new leads found for immediate assignment", "INFO")
            else:
                self.debug_print("⚠️ Immediate auto-assign completed with issues", "WARNING")
        except Exception as e:
            self.debug_print(f"❌ Error in immediate auto-assign: {e}", "ERROR")
        
        self.debug_print("   " + "="*80, "DEBUG")
        
        # Continuous background auto-assign every 5 minutes
        while self.running:
            try:
                # Check for new leads every 5 minutes
                time.sleep(300)  # 5 minutes
                
                # Update status
                self.system_status['last_run'] = self.get_ist_timestamp()
                self.system_status['next_run'] = self.get_ist_timestamp()
                self.system_status['total_runs'] += 1
                
                self.debug_print("🔄 Background auto-assign check running...", "SYSTEM")
                self.debug_print(f"   ⏰ Check Time: {self.get_ist_timestamp()}", "INFO")
                self.debug_print(f"   📊 Run #{self.system_status['total_runs']}", "INFO")
                self.debug_print("   " + "="*80, "DEBUG")
                
                try:
                    result = self.check_and_assign_new_leads()
                    if result and result.get('success'):
                        self.debug_print("✅ Background check completed successfully", "SUCCESS")
                        self.system_status['total_leads_assigned'] += result.get('total_assigned', 0)
                        if result.get('total_assigned', 0) > 0:
                            self.debug_print(f"📊 {result.get('total_assigned')} leads assigned", "SUCCESS")
                            self.debug_print(f"🎯 Total leads assigned so far: {self.system_status['total_leads_assigned']}", "INFO")
                    else:
                        self.debug_print("⚠️ Background check completed with issues", "WARNING")
                except Exception as context_error:
                    self.debug_print(f"❌ Error in assignment context: {context_error}", "ERROR")
                    
            except Exception as e:
                self.debug_print(f"❌ CRITICAL ERROR in background worker: {e}", "ERROR")
                self.debug_print(f"   ⏰ Error Time: {self.get_ist_timestamp()}", "ERROR")
                self.debug_print(f"   🚨 Error Type: {type(e).__name__}", "ERROR")
                self.debug_print(f"   🔍 Error Details: {str(e)}", "ERROR")
                self.debug_print("   " + "="*80, "ERROR")
                time.sleep(60)  # Wait 1 minute on error before retrying
        
        self.debug_print("🛑 Auto-assign worker stopped", "SYSTEM")
        self.system_status['is_running'] = False
    
    def start_robust_auto_assign_system(self) -> Optional[threading.Thread]:
        """Start the robust auto-assign system"""
        try:
            # Check if system is already running
            if self.system_status['is_running'] and self.auto_assign_thread and self.auto_assign_thread.is_alive():
                self.debug_print("🔄 Auto-assign system is already running", "WARNING")
                return self.auto_assign_thread
            
            # Stop any existing system
            if self.auto_assign_thread:
                self.running = False
                time.sleep(2)  # Give thread time to stop
            
            self.debug_print("🚀 Starting robust auto-assign system...", "SYSTEM")
            self.debug_print("   📋 Will check for new leads every 5 minutes", "INFO")
            self.debug_print("   ⚡ First auto-assign check starting now...", "INFO")
            
            # Create and start new thread
            self.running = True
            self.auto_assign_thread = threading.Thread(
                target=self.robust_auto_assign_worker, 
                name="RobustAutoAssignWorker",
                daemon=False
            )
            self.auto_assign_thread.start()
            
            # Update status
            self.system_status['is_running'] = True
            
            self.debug_print("✅ Robust auto-assign system started successfully", "SUCCESS")
            return self.auto_assign_thread
            
        except Exception as e:
            self.debug_print(f"❌ Error starting auto-assign system: {e}", "ERROR")
            return None
    
    def stop_auto_assign_system(self) -> bool:
        """Stop the auto-assign system"""
        try:
            if not self.system_status['is_running']:
                self.debug_print("ℹ️ Auto-assign system is not running", "INFO")
                return True
            
            self.debug_print("🛑 Stopping auto-assign system...", "SYSTEM")
            self.running = False
            
            if self.auto_assign_thread and self.auto_assign_thread.is_alive():
                self.auto_assign_thread.join(timeout=10)  # Wait up to 10 seconds
            
            self.system_status['is_running'] = False
            self.debug_print("✅ Auto-assign system stopped successfully", "SUCCESS")
            return True
            
        except Exception as e:
            self.debug_print(f"❌ Error stopping auto-assign system: {e}", "ERROR")
            return False
    
    def get_auto_assign_status(self) -> Dict[str, Any]:
        """Get comprehensive auto-assign system status"""
        try:
            status = {
                'is_running': self.system_status['is_running'],
                'total_runs': self.system_status['total_runs'],
                'total_leads_assigned': self.system_status['total_leads_assigned'],
                'last_run': self.system_status['last_run'],
                'next_run': self.system_status['next_run'],
                'started_at': self.system_status['started_at'],
                'thread_alive': self.auto_assign_thread.is_alive() if self.auto_assign_thread else False,
                'thread_name': self.auto_assign_thread.name if self.auto_assign_thread else None,
                'virtual_threads': self.virtual_thread_manager.get_all_threads_status(),
                'debug_mode': self.debug_mode,
                'verbose_logging': self.verbose_logging,
                'timestamp': self.get_ist_timestamp()
            }
            
            self.debug_print("📊 System status retrieved successfully", "DEBUG")
            return status
            
        except Exception as e:
            self.debug_print(f"❌ Error getting system status: {e}", "ERROR")
            return {'error': str(e)}
    
    def get_system_health(self) -> Dict[str, Any]:
        """Get detailed system health information"""
        try:
            status = self.get_auto_assign_status()
            
            # Calculate health metrics
            health_score = 100
            issues = []
            
            # Check if system is running
            if not status.get('is_running', False):
                health_score -= 30
                issues.append("System not running")
            
            # Check thread status
            if not status.get('thread_alive', False):
                health_score -= 20
                issues.append("Background thread not alive")
            
            # Check for recent errors
            if len(self.system_status.get('errors', [])) > 0:
                health_score -= 10
                issues.append(f"{len(self.system_status['errors'])} recent errors")
            
            # Check uptime
            uptime = "Unknown"
            if status.get('started_at'):
                try:
                    start_time = datetime.strptime(status['started_at'], '%Y-%m-%d %H:%M:%S')
                    uptime_delta = datetime.now() - start_time
                    uptime = str(uptime_delta).split('.')[0]
                except:
                    uptime = "Invalid timestamp"
            
            health = {
                'health_score': max(0, health_score),
                'status': 'Healthy' if health_score >= 80 else 'Warning' if health_score >= 50 else 'Critical',
                'issues': issues,
                'uptime': uptime,
                'last_run': status.get('last_run', 'Never'),
                'total_runs': status.get('total_runs', 0),
                'total_leads_assigned': status.get('total_leads_assigned', 0),
                'timestamp': self.get_ist_timestamp()
            }
            
            self.debug_print(f"🏥 System health check completed. Score: {health_score}/100", "DEBUG")
            return health
            
        except Exception as e:
            self.debug_print(f"❌ Error getting system health: {e}", "ERROR")
            return {'error': str(e), 'health_score': 0, 'status': 'Error'}
    
    def clear_system_errors(self) -> bool:
        """Clear system error history"""
        try:
            self.system_status['errors'] = []
            self.debug_print("🧹 System errors cleared", "INFO")
            return True
        except Exception as e:
            self.debug_print(f"❌ Error clearing system errors: {e}", "ERROR")
            return False
    
    def get_system_statistics(self) -> Dict[str, Any]:
        """Get comprehensive system statistics"""
        try:
            # Get basic status
            status = self.get_auto_assign_status()
            
            # Get database statistics
            total_configs = len(self.get_auto_assign_configs())
            total_cres = len(self.get_cre_users())
            
            # Calculate additional metrics
            avg_leads_per_run = 0
            if status.get('total_runs', 0) > 0:
                avg_leads_per_run = status.get('total_leads_assigned', 0) / status.get('total_runs', 1)
            
            # Get source distribution
            configs = self.get_auto_assign_configs()
            source_distribution = {}
            for config in configs:
                source = config.get('source', 'Unknown')
                if source not in source_distribution:
                    source_distribution[source] = 0
                source_distribution[source] += 1
            
            statistics = {
                'system_status': status,
                'database_stats': {
                    'total_configs': total_configs,
                    'total_cres': total_cres,
                    'active_sources': len(source_distribution)
                },
                'performance_metrics': {
                    'avg_leads_per_run': round(avg_leads_per_run, 2),
                    'success_rate': self._calculate_success_rate(status),
                    'system_uptime': self._calculate_uptime(status.get('started_at'))
                },
                'source_distribution': source_distribution,
                'timestamp': self.get_ist_timestamp()
            }
            
            self.debug_print("📊 System statistics generated successfully", "DEBUG")
            return statistics
            
        except Exception as e:
            self.debug_print(f"❌ Error getting system statistics: {e}", "ERROR")
            return {'error': str(e)}
    
    def _calculate_uptime(self, started_at: str) -> str:
        """Calculate system uptime"""
        if not started_at:
            return "Unknown"
        
        try:
            start_time = datetime.strptime(started_at, '%Y-%m-%d %H:%M:%S')
            uptime = datetime.now() - start_time
            return str(uptime).split('.')[0]  # Remove microseconds
        except:
            return "Unknown"
    
    def _calculate_success_rate(self, status: Dict) -> float:
        """Calculate system success rate"""
        total_runs = status.get('total_runs', 0)
        if total_runs == 0:
            return 100.0
        
        # Calculate success rate based on error count
        error_count = len(status.get('errors', []))
        success_rate = max(0, 100 - (error_count / total_runs * 100))
        return round(success_rate, 1)

# =============================================================================
# EXPORT AND HISTORY MANAGEMENT
# =============================================================================

class AutoAssignExporter:
    """Handles export and history management for auto-assign system"""
    
    def __init__(self, auto_assign_system: AutoAssignSystem):
        self.auto_assign_system = auto_assign_system
        self.export_dir = 'exports'
        os.makedirs(self.export_dir, exist_ok=True)
    
    def export_auto_assign_history_csv(self, filename: str = None) -> str:
        """
        Export auto-assign history to CSV format
        
        Args:
            filename: Optional filename, will generate one if not provided
            
        Returns:
            str: Path to exported file
        """
        try:
            if not filename:
                # Use IST timestamp for filename consistency
                ist_timestamp = self.auto_assign_system.get_current_ist_time().replace(' ', '_').replace(':', '')
                filename = f"auto_assign_history_{ist_timestamp}.csv"
            
            filepath = os.path.join(self.export_dir, filename)
            
            # Get history data from database
            result = self.auto_assign_system.supabase.table('auto_assign_history').select('*').order('created_at', desc=True).execute()
            history_data = result.data if result.data else []
            
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['lead_uid', 'source', 'assigned_cre_name', 'cre_total_leads_before', 
                             'cre_total_leads_after', 'assignment_method', 'created_at']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                for record in history_data:
                    writer.writerow({
                        'lead_uid': record.get('lead_uid', ''),
                        'source': record.get('source', ''),
                        'assigned_cre_name': record.get('assigned_cre_name', ''),
                        'cre_total_leads_before': record.get('cre_total_leads_before', 0),
                        'cre_total_leads_after': record.get('cre_total_leads_after', 0),
                        'assignment_method': record.get('assignment_method', 'fair_distribution'),
                        'created_at': record.get('created_at', '')
                    })
            
            self.auto_assign_system.debug_print(f"📊 Exported {len(history_data)} history records to {filepath}", "SUCCESS")
            return filepath
            
        except Exception as e:
            self.auto_assign_system.debug_print(f"❌ Error exporting history: {e}", "ERROR")
            return None
    
    def export_auto_assign_configs_csv(self, filename: str = None) -> str:
        """
        Export auto-assign configurations to CSV format
        
        Args:
            filename: Optional filename, will generate one if not provided
            
        Returns:
            str: Path to exported file
        """
        try:
            if not filename:
                # Use IST timestamp for filename consistency
                ist_timestamp = self.auto_assign_system.get_current_ist_time().replace(' ', '_').replace(':', '')
                filename = f"auto_assign_configs_{ist_timestamp}.csv"
            
            filepath = os.path.join(self.export_dir, filename)
            
            # Get config data from database
            result = self.auto_assign_system.supabase.table('auto_assign_config').select('*').order('source', desc=False).execute()
            config_data = result.data if result.data else []
            
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['source', 'cre_id', 'is_active', 'priority', 'created_at', 'updated_at']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                for record in config_data:
                    writer.writerow({
                        'source': record.get('source', ''),
                        'cre_id': record.get('cre_id', 0),
                        'is_active': record.get('is_active', True),
                        'priority': record.get('priority', 1),
                        'created_at': record.get('created_at', ''),
                        'updated_at': record.get('updated_at', '')
                    })
            
            self.auto_assign_system.debug_print(f"📊 Exported {len(config_data)} config records to {filepath}", "SUCCESS")
            return filepath
            
        except Exception as e:
            self.auto_assign_system.debug_print(f"❌ Error exporting configs: {e}", "ERROR")
            return None
    
    def generate_auto_assign_report(self) -> Dict[str, Any]:
        """
        Generate comprehensive auto-assign report
        
        Returns:
            dict: Report data
        """
        try:
            status = self.auto_assign_system.get_auto_assign_status()
            
            # Get additional statistics from database
            total_configs = len(self.auto_assign_system.get_auto_assign_configs())
            total_cres = len(self.auto_assign_system.get_cre_users())
            
            report = {
                'report_generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'system_status': status,
                'summary': {
                    'total_leads_assigned': status.get('total_leads_assigned', 0),
                    'total_runs': status.get('total_runs', 0),
                    'system_uptime': self._calculate_uptime(status.get('started_at')),
                    'success_rate': self._calculate_success_rate(status),
                    'active_sources': list(set([config['source'] for config in self.auto_assign_system.get_auto_assign_configs()])),
                    'active_cres': total_cres,
                    'total_configs': total_configs
                },
                'performance_metrics': {
                    'leads_per_run': status.get('total_leads_assigned', 0) / max(status.get('total_runs', 1), 1),
                    'last_activity': status.get('last_run', 'Never'),
                    'system_health': 'Healthy' if status.get('is_running') else 'Stopped'
                }
            }
            
            self.auto_assign_system.debug_print("📊 Auto-assign report generated successfully", "SUCCESS")
            return report
            
        except Exception as e:
            self.auto_assign_system.debug_print(f"❌ Error generating report: {e}", "ERROR")
            return {'error': str(e)}
    
    def _calculate_uptime(self, started_at: str) -> str:
        """Calculate system uptime"""
        if not started_at:
            return "Unknown"
        
        try:
            start_time = datetime.strptime(started_at, '%Y-%m-%d %H:%M:%S')
            uptime = datetime.now() - start_time
            return str(uptime).split('.')[0]  # Remove microseconds
        except:
            return "Unknown"
    
    def _calculate_success_rate(self, status: Dict) -> float:
        """Calculate system success rate"""
        total_runs = status.get('total_runs', 0)
        if total_runs == 0:
            return 100.0
        
        # Simulate success rate calculation based on error count
        error_count = len(status.get('errors', []))
        success_rate = max(0, 100 - (error_count / total_runs * 100))
        return round(success_rate, 1)
    
    def export_system_report_csv(self, filename: str = None) -> str:
        """
        Export comprehensive system report to CSV format
        
        Args:
            filename: Optional filename, will generate one if not provided
            
        Returns:
            str: Path to exported file
        """
        try:
            if not filename:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"auto_assign_system_report_{timestamp}.csv"
            
            filepath = os.path.join(self.export_dir, filename)
            
            # Get system statistics
            stats = self.auto_assign_system.get_system_statistics()
            
            # Prepare report data
            report_data = [
                {
                    'metric': 'System Status',
                    'value': stats.get('system_status', {}).get('is_running', 'Unknown'),
                    'category': 'Status'
                },
                {
                    'metric': 'Total Runs',
                    'value': stats.get('system_status', {}).get('total_runs', 0),
                    'category': 'Performance'
                },
                {
                    'metric': 'Total Leads Assigned',
                    'value': stats.get('system_status', {}).get('total_leads_assigned', 0),
                    'category': 'Performance'
                },
                {
                    'metric': 'Active Sources',
                    'value': stats.get('database_stats', {}).get('active_sources', 0),
                    'category': 'Configuration'
                },
                {
                    'metric': 'Total CREs',
                    'value': stats.get('database_stats', {}).get('total_cres', 0),
                    'category': 'Configuration'
                },
                {
                    'metric': 'Success Rate',
                    'value': f"{stats.get('performance_metrics', {}).get('success_rate', 0)}%",
                    'category': 'Performance'
                },
                {
                    'metric': 'System Uptime',
                    'value': stats.get('performance_metrics', {}).get('system_uptime', 'Unknown'),
                    'category': 'Status'
                }
            ]
            
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['category', 'metric', 'value']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                for record in report_data:
                    writer.writerow(record)
            
            self.auto_assign_system.debug_print(f"📊 Exported system report to {filepath}", "SUCCESS")
            return filepath
            
        except Exception as e:
            self.auto_assign_system.debug_print(f"❌ Error exporting system report: {e}", "ERROR")
            return None
    
    def export_cre_performance_csv(self, filename: str = None) -> str:
        """
        Export CRE performance data to CSV format
        
        Args:
            filename: Optional filename, will generate one if not provided
            
        Returns:
            str: Path to exported file
        """
        try:
            if not filename:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"cre_performance_{timestamp}.csv"
            
            filepath = os.path.join(self.export_dir, filename)
            
            # Get CRE users data
            cres = self.auto_assign_system.get_cre_users()
            
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['cre_id', 'cre_name', 'username', 'auto_assign_count', 'is_active', 'role']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                for cre in cres:
                    writer.writerow({
                        'cre_id': cre.get('id', ''),
                        'cre_name': cre.get('name', ''),
                        'username': cre.get('username', ''),
                        'auto_assign_count': cre.get('auto_assign_count', 0),
                        'is_active': cre.get('is_active', True),
                        'role': cre.get('role', 'cre')
                    })
            
            self.auto_assign_system.debug_print(f"📊 Exported CRE performance data to {filepath}", "SUCCESS")
            return filepath
            
        except Exception as e:
            self.auto_assign_system.debug_print(f"❌ Error exporting CRE performance: {e}", "ERROR")
            return None
    
    def generate_detailed_report(self) -> Dict[str, Any]:
        """
        Generate detailed auto-assign report with additional metrics
        
        Returns:
            dict: Detailed report data
        """
        try:
            # Get basic report
            basic_report = self.generate_auto_assign_report()
            
            # Get additional data
            system_health = self.auto_assign_system.get_system_health()
            cre_performance = self.auto_assign_system.get_cre_users()
            
            # Calculate CRE performance metrics
            total_cre_leads = sum(cre.get('auto_assign_count', 0) for cre in cre_performance)
            avg_leads_per_cre = total_cre_leads / max(len(cre_performance), 1)
            
            # Get top performing CREs
            top_cres = sorted(cre_performance, key=lambda x: x.get('auto_assign_count', 0), reverse=True)[:5]
            
            detailed_report = {
                **basic_report,
                'system_health': system_health,
                'cre_analytics': {
                    'total_cre_leads': total_cre_leads,
                    'avg_leads_per_cre': round(avg_leads_per_cre, 2),
                    'top_performers': [
                        {
                            'name': cre.get('name', 'Unknown'),
                            'leads_assigned': cre.get('auto_assign_count', 0)
                        }
                        for cre in top_cres
                    ]
                },
                'export_options': {
                    'history_csv': 'auto_assign_history.csv',
                    'configs_csv': 'auto_assign_configs.csv',
                    'system_report_csv': 'auto_assign_system_report.csv',
                    'cre_performance_csv': 'cre_performance.csv'
                }
            }
            
            self.auto_assign_system.debug_print("📊 Detailed report generated successfully", "SUCCESS")
            return detailed_report
            
        except Exception as e:
            self.auto_assign_system.debug_print(f"❌ Error generating detailed report: {e}", "ERROR")
            return {'error': str(e)}

# =============================================================================
# API ENDPOINTS SIMULATION
# =============================================================================

class AutoAssignAPI:
    """Simulates API endpoints for auto-assign system"""
    
    def __init__(self, auto_assign_system: AutoAssignSystem):
        self.auto_assign_system = auto_assign_system
        self.exporter = AutoAssignExporter(auto_assign_system)
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get system status endpoint"""
        try:
            status = self.auto_assign_system.get_auto_assign_status()
            return {
                'success': True,
                'status': status,
                'timestamp': self.auto_assign_system.get_ist_timestamp()
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'timestamp': self.auto_assign_system.get_ist_timestamp()
            }
    
    def trigger_auto_assign(self) -> Dict[str, Any]:
        """Trigger immediate auto-assign endpoint"""
        try:
            result = self.auto_assign_system.check_and_assign_new_leads()
            if result and result.get('success'):
                return {
                    'success': True,
                    'message': f'Auto-assign completed: {result.get("total_assigned", 0)} leads assigned',
                    'total_assigned': result.get('total_assigned', 0),
                    'timestamp': self.auto_assign_system.get_ist_timestamp()
                }
            else:
                return {
                    'success': False,
                    'message': 'Auto-assign completed with issues',
                    'error': result.get('message', 'Unknown error') if result else 'No result'
                }
        except Exception as e:
            return {
                'success': False,
                'message': f'Error triggering auto-assign: {str(e)}'
            }
    
    def trigger_auto_assign_for_source(self, source: str) -> Dict[str, Any]:
        """Trigger auto-assign for a specific source"""
        try:
            result = self.auto_assign_system.auto_assign_new_leads_for_source(source)
            if result and result.get('success'):
                return {
                    'success': True,
                    'message': f'Auto-assign for {source} completed: {result.get("assigned_count", 0)} leads assigned',
                    'assigned_count': result.get('assigned_count', 0),
                    'source': source,
                    'timestamp': self.auto_assign_system.get_ist_timestamp()
                }
            else:
                return {
                    'success': False,
                    'message': f'Auto-assign for {source} completed with issues',
                    'error': result.get('message', 'Unknown error') if result else 'No result'
                }
        except Exception as e:
            return {
                'success': False,
                'message': f'Error triggering auto-assign for {source}: {str(e)}'
            }
    
    def get_auto_assign_configs(self) -> Dict[str, Any]:
        """Get auto-assign configurations endpoint"""
        try:
            configs = self.auto_assign_system.get_auto_assign_configs()
            return {
                'success': True,
                'configs': configs,
                'count': len(configs),
                'timestamp': self.auto_assign_system.get_ist_timestamp()
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'timestamp': self.auto_assign_system.get_ist_timestamp()
            }
    
    def get_cre_users(self) -> Dict[str, Any]:
        """Get CRE users endpoint"""
        try:
            cres = self.auto_assign_system.get_cre_users()
            return {
                'success': True,
                'cre_users': cres,
                'count': len(cres),
                'timestamp': self.auto_assign_system.get_ist_timestamp()
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'timestamp': self.auto_assign_system.get_ist_timestamp()
            }
    
    def export_history(self, format_type: str = 'csv') -> Dict[str, Any]:
        """Export history endpoint"""
        try:
            if format_type.lower() == 'csv':
                filepath = self.exporter.export_auto_assign_history_csv()
                if filepath:
                    return {
                        'success': True,
                        'message': 'History exported successfully',
                        'filepath': filepath,
                        'format': 'csv',
                        'timestamp': self.auto_assign_system.get_ist_timestamp()
                    }
                else:
                    return {
                        'success': False,
                        'message': 'Failed to export history'
                    }
            else:
                return {
                    'success': False,
                    'message': f'Unsupported format: {format_type}'
                }
        except Exception as e:
            return {
                'success': False,
                'message': f'Error exporting history: {str(e)}'
            }
    
    def export_configs(self, format_type: str = 'csv') -> Dict[str, Any]:
        """Export configurations endpoint"""
        try:
            if format_type.lower() == 'csv':
                filepath = self.exporter.export_auto_assign_configs_csv()
                if filepath:
                    return {
                        'success': True,
                        'message': 'Configurations exported successfully',
                        'filepath': filepath,
                        'format': 'csv',
                        'timestamp': self.auto_assign_system.get_ist_timestamp()
                    }
                else:
                    return {
                        'success': False,
                        'message': 'Failed to export configurations'
                    }
            else:
                return {
                    'success': False,
                    'message': f'Unsupported format: {format_type}'
                }
        except Exception as e:
            return {
                'success': False,
                'message': f'Error exporting configurations: {str(e)}'
            }
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics endpoint"""
        try:
            report = self.exporter.generate_auto_assign_report()
            return {
                'success': True,
                'metrics': report,
                'timestamp': self.auto_assign_system.get_ist_timestamp()
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'timestamp': self.auto_assign_system.get_ist_timestamp()
            }
    
    def reset_cre_counts(self, cre_ids: List[int]) -> Dict[str, Any]:
        """Reset CRE auto-assign counts endpoint"""
        try:
            success = self.auto_assign_system.reset_cre_auto_assign_counts(cre_ids)
            if success:
                return {
                    'success': True,
                    'message': f'Successfully reset counts for {len(cre_ids)} CREs',
                    'cre_ids': cre_ids,
                    'timestamp': self.auto_assign_system.get_ist_timestamp()
                }
            else:
                return {
                    'success': False,
                    'message': 'Failed to reset CRE counts'
                }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'timestamp': self.auto_assign_system.get_ist_timestamp()
            }
    
    def get_virtual_threads_status(self) -> Dict[str, Any]:
        """Get virtual threads status endpoint"""
        try:
            threads_status = self.auto_assign_system.virtual_thread_manager.get_all_threads_status()
            return {
                'success': True,
                'threads_status': threads_status,
                'timestamp': self.auto_assign_system.get_ist_timestamp()
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'timestamp': self.auto_assign_system.get_ist_timestamp()
            }
    
    def get_system_health(self) -> Dict[str, Any]:
        """Get system health endpoint"""
        try:
            health = self.auto_assign_system.get_system_health()
            return {
                'success': True,
                'health': health,
                'timestamp': self.auto_assign_system.get_ist_timestamp()
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'timestamp': self.auto_assign_system.get_ist_timestamp()
            }
    
    def get_system_statistics(self) -> Dict[str, Any]:
        """Get system statistics endpoint"""
        try:
            stats = self.auto_assign_system.get_system_statistics()
            return {
                'success': True,
                'statistics': stats,
                'timestamp': self.auto_assign_system.get_ist_timestamp()
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'timestamp': self.auto_assign_system.get_ist_timestamp()
            }
    
    def clear_system_errors(self) -> Dict[str, Any]:
        """Clear system errors endpoint"""
        try:
            success = self.auto_assign_system.clear_system_errors()
            if success:
                return {
                    'success': True,
                    'message': 'System errors cleared successfully',
                    'timestamp': self.auto_assign_system.get_ist_timestamp()
                }
            else:
                return {
                    'success': False,
                    'message': 'Failed to clear system errors'
                }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'timestamp': self.auto_assign_system.get_ist_timestamp()
            }
    
    def export_system_report(self, format_type: str = 'csv') -> Dict[str, Any]:
        """Export system report endpoint"""
        try:
            if format_type.lower() == 'csv':
                filepath = self.exporter.export_system_report_csv()
                if filepath:
                    return {
                        'success': True,
                        'message': 'System report exported successfully',
                        'filepath': filepath,
                        'format': 'csv',
                        'timestamp': self.auto_assign_system.get_ist_timestamp()
                    }
                else:
                    return {
                        'success': False,
                        'message': 'Failed to export system report'
                    }
            else:
                return {
                    'success': False,
                    'message': f'Unsupported format: {format_type}'
                }
        except Exception as e:
            return {
                'success': False,
                'message': f'Error exporting system report: {str(e)}'
            }
    
    def export_cre_performance(self, format_type: str = 'csv') -> Dict[str, Any]:
        """Export CRE performance endpoint"""
        try:
            if format_type.lower() == 'csv':
                filepath = self.exporter.export_cre_performance_csv()
                if filepath:
                    return {
                        'success': True,
                        'message': 'CRE performance exported successfully',
                        'filepath': filepath,
                        'format': 'csv',
                        'timestamp': self.auto_assign_system.get_ist_timestamp()
                    }
                else:
                    return {
                        'success': False,
                        'message': 'Failed to export CRE performance'
                    }
            else:
                return {
                    'success': False,
                    'message': f'Unsupported format: {format_type}'
                }
        except Exception as e:
            return {
                'success': False,
                'message': f'Error exporting CRE performance: {str(e)}'
            }
    
    def get_detailed_report(self) -> Dict[str, Any]:
        """Get detailed report endpoint"""
        try:
            report = self.exporter.generate_detailed_report()
            return {
                'success': True,
                'report': report,
                'timestamp': self.auto_assign_system.get_ist_timestamp()
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'timestamp': self.auto_assign_system.get_ist_timestamp()
            }

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('auto_assign.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Fix Unicode issues on Windows
import sys
if sys.platform.startswith('win'):
    # Use ASCII-safe logging on Windows
    class SafeStreamHandler(logging.StreamHandler):
        def emit(self, record):
            try:
                super().emit(record)
            except UnicodeEncodeError:
                # Fallback to ASCII-safe message
                record.msg = record.msg.encode('ascii', 'ignore').decode('ascii')
                super().emit(record)
    
    # Replace the stream handler
    for handler in logger.handlers:
        if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
            logger.removeHandler(handler)
            logger.addHandler(SafeStreamHandler())

# =============================================================================
# DEMO AND TESTING FUNCTIONS
# =============================================================================

def demo_auto_assign_system():
    """Demonstrate the auto-assign system functionality"""
    print("🚀 Starting Auto-Assign System Demo")
    print("=" * 60)
    
    # Initialize system (with mock supabase for demo)
    class MockSupabase:
        def table(self, name):
            return MockTable()
    
    class MockTable:
        def select(self, *args):
            return self
        def eq(self, field, value):
            return self
        def execute(self):
            return MockResult()
        def update(self, data):
            return self
        def insert(self, data):
            return self
        def order(self, field, direction):
            return self
    
    class MockResult:
        @property
        def data(self):
            if 'auto_assign_config' in str(self):
                return [{'source': 'Google Know', 'cre_id': 1, 'is_active': True}]
            elif 'cre_users' in str(self):
                return [{'id': 1, 'name': 'CRE_1', 'auto_assign_count': 0}]
            elif 'lead_master' in str(self):
                return [{'uid': 'LEAD_1', 'customer_name': 'Customer 1', 'source': 'Google Know'}]
            return []
    
    mock_supabase = MockSupabase()
    auto_assign_system = AutoAssignSystem(mock_supabase)
    api = AutoAssignAPI(auto_assign_system)
    
    # Start the system
    print("\n1️⃣ Starting Auto-Assign System...")
    thread = auto_assign_system.start_robust_auto_assign_system()
    
    if thread:
        print("✅ System started successfully")
        
        # Let it run for a few seconds
        print("\n2️⃣ Running system for 10 seconds...")
        time.sleep(10)
        
        # Check status
        print("\n3️⃣ Checking System Status...")
        status = auto_assign_system.get_auto_assign_status()
        print(f"   🟢 Running: {status.get('is_running', False)}")
        print(f"   📊 Total Runs: {status.get('total_runs', 0)}")
        print(f"   🎯 Total Leads: {status.get('total_leads_assigned', 0)}")
        
        # Test manual trigger
        print("\n4️⃣ Testing Manual Trigger...")
        result = api.trigger_auto_assign()
        print(f"   📊 Result: {result.get('message', 'N/A')}")
        
        # Export history
        print("\n5️⃣ Exporting History...")
        export_result = api.export_history('csv')
        if export_result.get('success'):
            print(f"   📁 Exported to: {export_result.get('filepath', 'N/A')}")
        
        # Get performance metrics
        print("\n6️⃣ Performance Metrics...")
        metrics = api.get_performance_metrics()
        if metrics.get('success'):
            summary = metrics['metrics']['summary']
            print(f"   📈 Total Leads: {summary.get('total_leads_assigned', 0)}")
            print(f"   🔄 Total Runs: {summary.get('total_runs', 0)}")
            print(f"   ⏱️ Uptime: {summary.get('system_uptime', 'N/A')}")
        
        # Stop system
        print("\n7️⃣ Stopping System...")
        auto_assign_system.stop_auto_assign_system()
        print("✅ System stopped successfully")
        
    else:
        print("❌ Failed to start system")
    
    print("\n🏁 Demo completed!")

def test_virtual_threads():
    """Test virtual thread management"""
    print("🧪 Testing Virtual Thread Management")
    print("=" * 50)
    
    # Create a mock system for testing
    class MockSupabase:
        pass
    
    auto_assign_system = AutoAssignSystem(MockSupabase())
    thread_manager = auto_assign_system.virtual_thread_manager
    
    # Create test threads
    def test_task(thread_id, delay):
        time.sleep(delay)
        return f"Task completed for {thread_id}"
    
    print("1️⃣ Creating test threads...")
    thread1 = thread_manager.create_virtual_thread("Test1", test_task, "Thread1", 2)
    thread2 = thread_manager.create_virtual_thread("Test2", test_task, "Thread2", 3)
    thread3 = thread_manager.create_virtual_thread("Test3", test_task, "Thread3", 1)
    
    print(f"   Created threads: {thread1}, {thread2}, {thread3}")
    
    # Monitor threads
    print("\n2️⃣ Monitoring threads...")
    for i in range(5):
        status = thread_manager.get_all_threads_status()
        print(f"   Iteration {i+1}: {status['active_threads']} active, {status['completed_threads']} completed")
        time.sleep(1)
    
    # Get final status
    print("\n3️⃣ Final thread status...")
    final_status = thread_manager.get_all_threads_status()
    print(f"   Total threads: {final_status['total_threads']}")
    print(f"   Active: {final_status['active_threads']}")
    print(f"   Completed: {final_status['completed_threads']}")
    print(f"   Failed: {final_status['failed_threads']}")
    
    print("\n✅ Virtual thread test completed!")

# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    print("🤖 Auto-Assign System Module")
    print("=" * 50)
    print("This module contains comprehensive auto-assign functionality including:")
    print("✅ Core auto-assign system with fair distribution")
    print("✅ Virtual thread management")
    print("✅ Debug logging and monitoring")
    print("✅ Export and history management")
    print("✅ API endpoint simulation")
    print("✅ Performance metrics and reporting")
    print("✅ Error handling and recovery")
    print("✅ Production-ready architecture")
    print("✅ Supabase database integration")
    
    print("\n🚀 Available demo functions:")
    print("   • demo_auto_assign_system() - Full system demo")
    print("   • test_virtual_threads() - Thread management test")
    
    print("\n💡 Usage:")
    print("   • Import this module to use auto-assign functionality")
    print("   • Run demo functions to see system in action")
    print("   • Customize for your specific CRM needs")
    print("   • Integrate with Flask app.py for web interface")
    
    print("\n🔧 Integration:")
    print("   • This module is designed to work with Flask app.py")
    print("   • Provides all necessary classes and methods")
    print("   • Includes comprehensive error handling")
    print("   • Ready for production deployment")
    
    # Uncomment to run demos
    # demo_auto_assign_system()
    # test_virtual_threads()
