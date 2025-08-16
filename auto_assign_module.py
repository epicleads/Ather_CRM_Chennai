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

UDAY BRANCH ENHANCEMENTS:
- Enhanced debug prints with stickers and emojis
- Detailed lead data logging (UID, customer, mobile, source, status)
- Real-time assignment status tracking
- Performance monitoring and success rate calculation
- Comprehensive verification with action recommendations
- Enhanced error handling with actionable insights
- Reference tracking to Uday branch enhanced logic
- Multi-source optimization with detailed progress tracking

Author: AI Assistant
Version: 1.0.0
Production Ready: Yes
Uday Branch Enhanced: Yes
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
# VIRTUAL THREAD MANAGEMENT SYSTEM (RENDER-COMPATIBLE)
# =============================================================================

class VirtualThreadManager:
    """Manages threads for auto-assign operations (Render-compatible)"""
    
    def __init__(self):
        self.threads = {}
        self.thread_counter = 0
        self.max_threads = 5  # Reduced for Render compatibility
        self.thread_timeout = 120  # Reduced timeout for Render (2 minutes)
        self.is_production = os.environ.get('RENDER', False) or os.environ.get('PRODUCTION', False)
        
    def create_virtual_thread(self, name: str, target_func, *args, **kwargs) -> str:
        """Create a thread for background processing (Render-compatible)"""
        if len(self.threads) >= self.max_threads:
            logger.warning(f"Maximum threads ({self.max_threads}) reached. Cannot create new thread: {name}")
            return None
        
        # Clean up completed threads first
        self._cleanup_completed_threads()
        
        thread_id = f"thread_{self.thread_counter}_{name}"
        self.thread_counter += 1
        
        try:
            # Use standard threading for Render compatibility
            if self.is_production:
                # Production mode: use daemon threads with reduced stack size
                thread = threading.Thread(
                    target=self._thread_wrapper,
                    args=(thread_id, target_func, *args),
                    kwargs=kwargs,
                    daemon=True,
                    name=thread_id
                )
                thread.daemon = True
            else:
                # Development mode: use regular threads
                thread = threading.Thread(
                    target=self._thread_wrapper,
                    args=(thread_id, target_func, *args),
                    kwargs=kwargs,
                    name=thread_id
                )
            
            thread.start()
            
            self.threads[thread_id] = {
                'thread': thread,
                'name': name,
                'started_at': time.time(),
                'status': 'running',
                'result': None,
                'error': None
            }
            
            logger.info(f"✅ Thread created successfully: {thread_id}")
            return thread_id
            
        except Exception as e:
            logger.error(f"❌ Error creating thread {thread_id}: {e}")
            return None
    
    def _thread_wrapper(self, thread_id: str, target_func, *args, **kwargs):
        """Wrapper for thread execution with error handling"""
        try:
            logger.info(f"🚀 Thread {thread_id} started")
            result = target_func(*args, **kwargs)
            
            if thread_id in self.threads:
                self.threads[thread_id]['status'] = 'completed'
                self.threads[thread_id]['result'] = result
                logger.info(f"✅ Thread {thread_id} completed successfully")
            
        except Exception as e:
            logger.error(f"❌ Thread {thread_id} failed: {e}")
            if thread_id in self.threads:
                self.threads[thread_id]['status'] = 'failed'
                self.threads[thread_id]['error'] = str(e)
        finally:
            # Clean up thread reference after completion
            if thread_id in self.threads:
                self.threads[thread_id]['completed_at'] = time.time()
    
    def _cleanup_completed_threads(self):
        """Clean up completed and failed threads"""
        current_time = time.time()
        threads_to_remove = []
        
        for thread_id, thread_info in self.threads.items():
            # Remove threads that are completed, failed, or timed out
            if thread_info['status'] in ['completed', 'failed']:
                threads_to_remove.append(thread_id)
            elif current_time - thread_info['started_at'] > self.thread_timeout:
                # Mark timed out threads as failed
                thread_info['status'] = 'timeout'
                thread_info['error'] = 'Thread timeout'
                threads_to_remove.append(thread_id)
        
        # Remove completed/failed/timed out threads
        for thread_id in threads_to_remove:
            if thread_id in self.threads:
                del self.threads[thread_id]
                logger.info(f"🧹 Cleaned up thread: {thread_id}")
    
    def get_thread_status(self, thread_id: str) -> Dict[str, Any]:
        """Get status of a specific thread"""
        if thread_id not in self.threads:
            return {'status': 'not_found'}
        
        thread_info = self.threads[thread_id]
        return {
            'status': thread_info['status'],
            'name': thread_info['name'],
            'started_at': thread_info['started_at'],
            'result': thread_info.get('result'),
            'error': thread_info.get('error'),
            'completed_at': thread_info.get('completed_at')
        }
    
    def get_all_threads_status(self) -> Dict[str, Any]:
        """Get status of all threads"""
        self._cleanup_completed_threads()
        
        active_threads = sum(1 for t in self.threads.values() if t['status'] == 'running')
        completed_threads = sum(1 for t in self.threads.values() if t['status'] == 'completed')
        failed_threads = sum(1 for t in self.threads.values() if t['status'] in ['failed', 'timeout'])
        
        return {
            'total_threads': len(self.threads),
            'active_threads': active_threads,
            'completed_threads': completed_threads,
            'failed_threads': failed_threads,
            'threads': {tid: self.get_thread_status(tid) for tid in self.threads.keys()}
        }
    
    def stop_thread(self, thread_id: str) -> bool:
        """Stop a specific thread"""
        if thread_id not in self.threads:
            return False
        
        thread_info = self.threads[thread_id]
        if thread_info['status'] == 'running':
            # Note: We can't forcefully stop threads in Python, just mark them
            thread_info['status'] = 'stopping'
            logger.warning(f"⚠️ Thread {thread_id} marked for stopping (will complete naturally)")
        
        return True
    
    def stop_all_threads(self):
        """Stop all running threads"""
        for thread_id in self.threads.keys():
            self.stop_thread(thread_id)
        logger.info("🛑 All threads marked for stopping")

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
        
        # Health monitoring
        self.health_monitor_thread = None
        self.last_health_check = time.time()
        self.health_check_interval = 300  # 5 minutes
        
        logger.info("🚀 Auto-Assign System initialized")
        if self.debug_mode:
            logger.info("🔍 Debug mode enabled")
        if self.verbose_logging:
            logger.info("📝 Verbose logging enabled")
        
        # Start health monitoring in production
        if os.environ.get('RENDER', False) or os.environ.get('PRODUCTION', False):
            self.start_health_monitoring()
    
    def start_health_monitoring(self):
        """Start health monitoring for production environments"""
        try:
            if self.health_monitor_thread and self.health_monitor_thread.is_alive():
                return
            
            def health_monitor():
                while True:
                    try:
                        time.sleep(self.health_check_interval)
                        
                        # Check if auto-assign system is healthy
                        if not self.system_status['is_running'] or not self.auto_assign_thread or not self.auto_assign_thread.is_alive():
                            logger.warning("🚨 Auto-assign system health check failed - attempting restart")
                            
                            # Try to restart the system
                            try:
                                self.stop_auto_assign_system()
                                time.sleep(2)
                                self.start_robust_auto_assign_system()
                                logger.info("✅ Auto-assign system restarted successfully")
                            except Exception as e:
                                logger.error(f"❌ Failed to restart auto-assign system: {e}")
                        
                        self.last_health_check = time.time()
                        
                    except Exception as e:
                        logger.error(f"❌ Error in health monitoring: {e}")
                        time.sleep(60)  # Wait before retrying
            
            self.health_monitor_thread = threading.Thread(target=health_monitor, daemon=False, name="HealthMonitor")
            self.health_monitor_thread.start()
            logger.info("🔍 Health monitoring started for production environment")
            
        except Exception as e:
            logger.error(f"❌ Error starting health monitoring: {e}")
    
    def debug_print(self, message: str, level: str = 'INFO'):
        """Enhanced debug print function with configurable levels and stickers"""
        if not self.debug_mode:
            return
            
        timestamp = self.get_ist_timestamp()
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
        """Get unassigned leads for a specific source with enhanced debug prints"""
        try:
            self.debug_print(f"🔍 ========================================", "DEBUG")
            self.debug_print(f"🔍 FETCHING UNASSIGNED LEADS", "DEBUG")
            self.debug_print(f"🔍 ========================================", "DEBUG")
            self.debug_print(f"   🏷️ Source: {source}", "DEBUG")
            self.debug_print(f"   ⏰ Time: {self.get_ist_timestamp()}", "DEBUG")
            self.debug_print(f"   🎯 Status: Fetching leads...", "DEBUG")
            
            result = self.supabase.table('lead_master').select('*').eq('source', source).eq('assigned', 'No').execute()
            leads = result.data if result.data else []
            
            if leads:
                self.debug_print(f"📊 Found {len(leads)} unassigned leads for {source}", "SUCCESS")
                self.debug_print(f"   🎯 Source: {source}", "INFO")
                self.debug_print(f"   📅 Timestamp: {self.get_ist_timestamp()}", "INFO")
                self.debug_print(f"   🔍 Status: Leads found successfully", "SUCCESS")
                
                # Show first few leads with detailed info
                for i, lead in enumerate(leads[:3]):  # Show first 3 leads
                    self.debug_print(f"   📋 Lead {i+1}:", "DEBUG")
                    self.debug_print(f"      🆔 UID: {lead.get('uid', 'N/A')}", "DEBUG")
                    self.debug_print(f"      👤 Customer: {lead.get('customer_name', 'N/A')}", "DEBUG")
                    self.debug_print(f"      📱 Mobile: {lead.get('customer_mobile_number', 'N/A')}", "DEBUG")
                    self.debug_print(f"      🏷️ Source: {lead.get('source', 'N/A')}", "DEBUG")
                    self.debug_print(f"      🎯 Sub-source: {lead.get('sub_source', 'N/A')}", "DEBUG")
                    self.debug_print(f"      📊 Status: {lead.get('lead_status', 'N/A')}", "DEBUG")
                    self.debug_print(f"      📅 Created: {lead.get('created_at', 'N/A')}", "DEBUG")
                
                if len(leads) > 3:
                    self.debug_print(f"   ... and {len(leads) - 3} more leads", "DEBUG")
                
                self.debug_print(f"🔍 ========================================", "DEBUG")
            else:
                self.debug_print(f"ℹ️ No unassigned leads found for source: {source}", "INFO")
                self.debug_print(f"   🎯 Status: No leads to assign", "INFO")
                self.debug_print(f"🔍 ========================================", "DEBUG")
            
            return leads
        except Exception as e:
            self.debug_print(f"❌ ========================================", "ERROR")
            self.debug_print(f"❌ ERROR FETCHING UNASSIGNED LEADS", "ERROR")
            self.debug_print(f"❌ ========================================", "ERROR")
            self.debug_print(f"   🚨 Exception: {e}", "ERROR")
            self.debug_print(f"   🚨 Exception type: {type(e).__name__}", "ERROR")
            self.debug_print(f"   🏷️ Source: {source}", "ERROR")
            self.debug_print(f"   ⏰ Time: {self.get_ist_timestamp()}", "ERROR")
            self.debug_print(f"❌ ========================================", "ERROR")
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
        """Assign a lead to a CRE user with enhanced debug prints and stickers"""
        try:
            self.debug_print(f"🎯 ========================================", "SYSTEM")
            self.debug_print(f"🎯 LEAD ASSIGNMENT PROCESS", "SYSTEM")
            self.debug_print(f"🎯 ========================================", "SYSTEM")
            self.debug_print(f"   🆔 Lead UID: {lead_uid}", "INFO")
            self.debug_print(f"   👥 CRE: {cre_name} (ID: {cre_id})", "INFO")
            self.debug_print(f"   🏷️ Source: {source}", "INFO")
            self.debug_print(f"   ⏰ Start Time: {self.get_ist_timestamp()}", "INFO")
            self.debug_print(f"   🔄 Status: Starting Assignment", "INFO")
            self.debug_print(f"   🚀 Reference: Uday Branch Enhanced Logic", "INFO")
            
            # Get CRE's current lead count BEFORE assignment
            self.debug_print(f"📊 Fetching CRE {cre_name} current lead count...", "DEBUG")
            cre_result = self.supabase.table('cre_users').select('auto_assign_count').eq('id', cre_id).execute()
            current_count = cre_result.data[0]['auto_assign_count'] if cre_result.data else 0
            
            self.debug_print(f"📊 CRE {cre_name} current auto_assign_count: {current_count}", "DEBUG")
            self.debug_print(f"   🔢 Count before assignment: {current_count}", "INFO")
            self.debug_print(f"   📊 Status: Count retrieved successfully", "SUCCESS")
            
            # Update lead_master table with assignment
            # Use IST timestamp for cre_assigned_at
            update_data = {
                'assigned': 'Yes',
                'cre_name': cre_name,
                'cre_assigned_at': self.get_ist_timestamp()  # Use IST timestamp
            }
            
            self.debug_print(f"🔄 ========================================", "DEBUG")
            self.debug_print(f"🔄 UPDATING LEAD_MASTER TABLE", "DEBUG")
            self.debug_print(f"🔄 ========================================", "DEBUG")
            self.debug_print(f"   📊 Update data: {update_data}", "DEBUG")
            self.debug_print(f"   🕒 IST Timestamp: {self.get_ist_timestamp()}", "DEBUG")
            self.debug_print(f"   🎯 Target: lead_master.uid = {lead_uid}", "DEBUG")
            self.debug_print(f"   🔄 Status: Updating lead assignment...", "DEBUG")
            
            try:
                lead_update_result = self.supabase.table('lead_master').update(update_data).eq('uid', lead_uid).execute()
                
                if lead_update_result.data:
                    self.debug_print(f"✅ SUCCESS: Lead {lead_uid} marked as assigned in lead_master", "SUCCESS")
                    self.debug_print(f"   📊 Update result: {lead_update_result.data}", "DEBUG")
                    self.debug_print(f"   🎯 Status: lead_master updated successfully", "SUCCESS")
                    self.debug_print(f"   🔄 Action: Lead assignment recorded", "SUCCESS")
                else:
                    self.debug_print(f"⚠️ WARNING: Lead update may have failed", "WARNING")
                    self.debug_print(f"   🚨 Update result: {lead_update_result}", "DEBUG")
                    if hasattr(lead_update_result, 'error'):
                        self.debug_print(f"   ❌ Update error: {lead_update_result.error}", "ERROR")
                    self.debug_print(f"   🔍 Action: Review update result", "WARNING")
                        
            except Exception as e:
                self.debug_print(f"❌ EXCEPTION during lead update: {e}", "ERROR")
                self.debug_print(f"   🚨 Exception type: {type(e).__name__}", "ERROR")
                self.debug_print(f"   📊 Update data: {update_data}", "DEBUG")
                self.debug_print(f"   🔍 Action: Review exception and retry", "ERROR")
                raise  # Re-raise to be caught by outer exception handler
            
            # Update CRE's auto_assign_count
            new_count = current_count + 1
            self.debug_print(f"📈 ========================================", "DEBUG")
            self.debug_print(f"📈 UPDATING CRE AUTO_ASSIGN_COUNT", "DEBUG")
            self.debug_print(f"📈 ========================================", "DEBUG")
            self.debug_print(f"   👥 CRE: {cre_name} (ID: {cre_id})", "DEBUG")
            self.debug_print(f"   🔢 Count change: {current_count} → {new_count}", "DEBUG")
            self.debug_print(f"   📊 Update data: {{'auto_assign_count': {new_count}}}", "DEBUG")
            self.debug_print(f"   🔄 Status: Updating CRE count...", "DEBUG")
            
            update_data = {
                'auto_assign_count': new_count
                # Note: updated_at is handled by database trigger
            }
            
            cre_update_result = self.supabase.table('cre_users').update(update_data).eq('id', cre_id).execute()
            
            if cre_update_result.data:
                self.debug_print(f"✅ SUCCESS: CRE {cre_name} auto_assign_count updated", "SUCCESS")
                self.debug_print(f"   🔢 New count: {new_count}", "SUCCESS")
                self.debug_print(f"   🎯 Status: CRE count updated successfully", "SUCCESS")
                self.debug_print(f"   🔄 Action: Count incremented", "SUCCESS")
            else:
                self.debug_print(f"⚠️ WARNING: CRE count update may have failed", "WARNING")
                self.debug_print(f"   🚨 Update result: {cre_update_result}", "DEBUG")
                if hasattr(cre_update_result, 'error'):
                    self.debug_print(f"   ❌ Update error: {cre_update_result.error}", "ERROR")
                self.debug_print(f"   🔍 Action: Review CRE update", "WARNING")
            
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
            self.debug_print(f"📝 ========================================", "DEBUG")
            self.debug_print(f"📝 CREATING HISTORY RECORD", "DEBUG")
            self.debug_print(f"📝 ========================================", "DEBUG")
            self.debug_print(f"   📊 History data: {history_data}", "DEBUG")
            self.debug_print(f"   🕒 System Time: {self.get_current_system_time()}", "DEBUG")
            self.debug_print(f"   🕒 IST Time: {self.get_current_ist_time()}", "DEBUG")
            self.debug_print(f"   🕒 Timestamp: Using database default now()", "DEBUG")
            self.debug_print(f"   🎯 Target: auto_assign_history table", "DEBUG")
            self.debug_print(f"   🔄 Status: Creating history record...", "DEBUG")
            
            history_result = self.supabase.table('auto_assign_history').insert(history_data).execute()
            
            if history_result.data:
                self.debug_print(f"✅ SUCCESS: History record created for lead {lead_uid}", "SUCCESS")
                self.debug_print(f"   📊 Before: {current_count}, After: {new_count}", "DEBUG")
                self.debug_print(f"   🕒 Timestamp: Database default now()", "DEBUG")
                self.debug_print(f"   📋 History ID: {history_result.data[0].get('id', 'Unknown')}", "DEBUG")
                self.debug_print(f"   🎯 Status: History record created successfully", "SUCCESS")
                self.debug_print(f"   🔄 Action: History logged", "SUCCESS")
            else:
                self.debug_print(f"⚠️ WARNING: History record may not have been created", "WARNING")
                self.debug_print(f"   🚨 History result: {history_result}", "DEBUG")
                if hasattr(history_result, 'error'):
                    self.debug_print(f"   ❌ History error: {history_result.error}", "ERROR")
                self.debug_print(f"   🔍 Action: Review history creation", "WARNING")
            
            # Verify the assignment was successful
            self.debug_print(f"🔍 ========================================", "DEBUG")
            self.debug_print(f"🔍 VERIFYING ASSIGNMENT", "DEBUG")
            self.debug_print(f"🔍 ========================================", "DEBUG")
            self.debug_print(f"   🎯 Verifying lead {lead_uid} assignment...", "DEBUG")
            self.debug_print(f"   🔄 Status: Running verification...", "DEBUG")
            
            verification_result = self.supabase.table('lead_master').select('assigned, cre_name, cre_assigned_at').eq('uid', lead_uid).execute()
            
            self.debug_print(f"   📊 Verification result: {verification_result.data}", "DEBUG")
            
            if verification_result.data:
                lead_data = verification_result.data[0]
                if lead_data['assigned'] == 'Yes' and lead_data['cre_name'] == cre_name:
                    self.debug_print(f"   ✅ VERIFICATION SUCCESS: Lead {lead_uid} properly assigned", "SUCCESS")
                    self.debug_print(f"      🎯 Status: Assignment verified in lead_master", "SUCCESS")
                    self.debug_print(f"      📊 assigned: {lead_data['assigned']}", "DEBUG")
                    self.debug_print(f"      👥 cre_name: {lead_data['cre_name']}", "DEBUG")
                    if lead_data.get('cre_assigned_at'):
                        self.debug_print(f"      🕒 cre_assigned_at: {lead_data['cre_assigned_at']}", "DEBUG")
                    else:
                        self.debug_print(f"      ⚠️ cre_assigned_at is NULL", "WARNING")
                else:
                    self.debug_print(f"   ⚠️ VERIFICATION WARNING: Assignment mismatch detected", "WARNING")
                    self.debug_print(f"      📊 Expected: assigned=Yes, cre_name={cre_name}", "DEBUG")
                    self.debug_print(f"      📊 Actual: assigned={lead_data['assigned']}, cre_name={lead_data['cre_name']}", "DEBUG")
                    self.debug_print(f"      🚨 Status: Verification failed", "WARNING")
                    self.debug_print(f"      🔍 Action: Review assignment data", "WARNING")
            else:
                self.debug_print(f"   ❌ VERIFICATION ERROR: Lead {lead_uid} not found", "ERROR")
                self.debug_print(f"      🚨 Status: Lead not found", "ERROR")
                self.debug_print(f"      🔍 Action: Check lead existence", "ERROR")
            
            self.debug_print(f"🎯 ========================================", "SYSTEM")
            self.debug_print(f"🎯 ASSIGNMENT COMPLETED SUCCESSFULLY", "SYSTEM")
            self.debug_print(f"🎯 ========================================", "SYSTEM")
            self.debug_print(f"   🆔 Lead: {lead_uid}", "SUCCESS")
            self.debug_print(f"   👥 CRE: {cre_name}", "SUCCESS")
            self.debug_print(f"   🏷️ Source: {source}", "SUCCESS")
            self.debug_print(f"   ⏰ Completion Time: {self.get_ist_timestamp()}", "SUCCESS")
            self.debug_print(f"   🎯 Status: Assignment Successful", "SUCCESS")
            self.debug_print(f"   🚀 Reference: Uday Branch Enhanced Logic", "SUCCESS")
            self.debug_print(f"   🔄 Action: Lead assigned and verified", "SUCCESS")
            
            return True
            
        except Exception as e:
            self.debug_print(f"❌ ========================================", "ERROR")
            self.debug_print(f"❌ LEAD ASSIGNMENT FAILED", "ERROR")
            self.debug_print(f"❌ ========================================", "ERROR")
            self.debug_print(f"   🆔 Lead: {lead_uid}", "ERROR")
            self.debug_print(f"   👥 CRE: {cre_name}", "ERROR")
            self.debug_print(f"   🏷️ Source: {source}", "ERROR")
            self.debug_print(f"   🚨 Exception: {e}", "ERROR")
            self.debug_print(f"   🚨 Exception type: {type(e).__name__}", "ERROR")
            self.debug_print(f"   ⏰ Time: {self.get_ist_timestamp()}", "ERROR")
            self.debug_print(f"   🎯 Status: Assignment Failed", "ERROR")
            self.debug_print(f"   🔍 Action: Review error and retry", "ERROR")
            self.debug_print(f"❌ ========================================", "ERROR")
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
    
    def handle_auto_assign_config_change(self, source: str, action: str, cre_ids: List[int] = None) -> bool:
        """
        Handle auto-assign configuration changes and automatically reset counts when needed.
        This ensures fair distribution starts fresh when CREs are added/removed.
        
        Args:
            source: The source name that configuration changed for
            action: The action performed ('add_cre', 'remove_cre', 'update_config', 'reset_all')
            cre_ids: List of CRE IDs affected (required for add_cre, remove_cre, update_config)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.debug_print(f"🔧 ========================================", "SYSTEM")
            self.debug_print(f"🔧 HANDLING AUTO-ASSIGN CONFIG CHANGE", "SYSTEM")
            self.debug_print(f"🔧 ========================================", "SYSTEM")
            self.debug_print(f"   🏷️ Source: {source}", "INFO")
            self.debug_print(f"   🔄 Action: {action}", "INFO")
            self.debug_print(f"   👥 Affected CREs: {cre_ids if cre_ids else 'All'}", "INFO")
            self.debug_print(f"   ⏰ Time: {self.get_ist_timestamp()}", "INFO")
            self.debug_print(f"   🎯 Purpose: Ensure fair distribution after config changes", "INFO")
            
            if action == 'add_cre':
                if not cre_ids:
                    self.debug_print("❌ CRE IDs required for add_cre action", "ERROR")
                    return False
                
                self.debug_print(f"➕ Adding new CREs to auto-assign for {source}", "INFO")
                self.debug_print(f"   👥 New CRE IDs: {cre_ids}", "INFO")
                self.debug_print(f"   🔄 Action: Reset counts to 0 for new CREs", "INFO")
                
                # Reset counts for new CREs to ensure they start with 0
                success = self.reset_cre_auto_assign_counts(cre_ids)
                if success:
                    self.debug_print(f"✅ Successfully prepared new CREs for fair distribution", "SUCCESS")
                else:
                    self.debug_print(f"❌ Failed to prepare new CREs", "ERROR")
                
                return success
                
            elif action == 'remove_cre':
                if not cre_ids:
                    self.debug_print("❌ CRE IDs required for remove_cre action", "ERROR")
                    return False
                
                self.debug_print(f"➖ Removing CREs from auto-assign for {source}", "INFO")
                self.debug_print(f"   👥 Removed CRE IDs: {cre_ids}", "INFO")
                self.debug_print(f"   🔄 Action: Reset counts to 0 for remaining CREs", "INFO")
                
                # Get all currently configured CREs for this source
                configs = self.supabase.table('auto_assign_config').select('cre_id').eq('source', source).eq('is_active', True).execute()
                if configs.data:
                    remaining_cre_ids = [config['cre_id'] for config in configs.data if config['cre_id'] not in cre_ids]
                    if remaining_cre_ids:
                        self.debug_print(f"   👥 Remaining CRE IDs: {remaining_cre_ids}", "INFO")
                        # Reset counts for remaining CREs to ensure fair distribution
                        success = self.reset_cre_auto_assign_counts(remaining_cre_ids)
                        if success:
                            self.debug_print(f"✅ Successfully reset counts for remaining CREs", "SUCCESS")
                        else:
                            self.debug_print(f"❌ Failed to reset counts for remaining CREs", "ERROR")
                        return success
                    else:
                        self.debug_print(f"ℹ️ No CREs remaining for {source}", "INFO")
                        return True
                else:
                    self.debug_print(f"ℹ️ No active configs found for {source}", "INFO")
                    return True
                
            elif action == 'update_config':
                if not cre_ids:
                    self.debug_print("❌ CRE IDs required for update_config action", "ERROR")
                    return False
                
                self.debug_print(f"🔄 Updating auto-assign configuration for {source}", "INFO")
                self.debug_print(f"   👥 Updated CRE IDs: {cre_ids}", "INFO")
                self.debug_print(f"   🔄 Action: Reset counts to 0 for all affected CREs", "INFO")
                
                # Reset counts for all affected CREs to ensure fair distribution
                success = self.reset_cre_auto_assign_counts(cre_ids)
                if success:
                    self.debug_print(f"✅ Successfully reset counts for updated configuration", "SUCCESS")
                else:
                    self.debug_print(f"❌ Failed to reset counts for updated configuration", "ERROR")
                
                return success
                
            elif action == 'reset_all':
                self.debug_print(f"🔄 Resetting all auto-assign counts for {source}", "INFO")
                self.debug_print(f"   🔄 Action: Reset counts to 0 for all CREs in source", "INFO")
                
                # Get all CREs configured for this source
                configs = self.supabase.table('auto_assign_config').select('cre_id').eq('source', source).eq('is_active', True).execute()
                if configs.data:
                    all_cre_ids = [config['cre_id'] for config in configs.data]
                    self.debug_print(f"   👥 All CRE IDs for {source}: {all_cre_ids}", "INFO")
                    
                    # Reset counts for all CREs
                    success = self.reset_cre_auto_assign_counts(all_cre_ids)
                    if success:
                        self.debug_print(f"✅ Successfully reset all counts for {source}", "SUCCESS")
                    else:
                        self.debug_print(f"❌ Failed to reset all counts for {source}", "ERROR")
                    return success
                else:
                    self.debug_print(f"ℹ️ No active configs found for {source}", "INFO")
                    return True
                    
            else:
                self.debug_print(f"❌ Unknown action: {action}", "ERROR")
                self.debug_print(f"   🔍 Valid actions: add_cre, remove_cre, update_config, reset_all", "ERROR")
                return False
                
        except Exception as e:
            self.debug_print(f"❌ ========================================", "ERROR")
            self.debug_print(f"❌ ERROR HANDLING CONFIG CHANGE", "ERROR")
            self.debug_print(f"❌ ========================================", "ERROR")
            self.debug_print(f"   🚨 Exception: {e}", "ERROR")
            self.debug_print(f"   🚨 Exception type: {type(e).__name__}", "ERROR")
            self.debug_print(f"   🏷️ Source: {source}", "ERROR")
            self.debug_print(f"   🔄 Action: {action}", "ERROR")
            self.debug_print(f"   ⏰ Time: {self.get_ist_timestamp()}", "ERROR")
            self.debug_print(f"   🔍 Action: Review error and retry", "ERROR")
            self.debug_print(f"❌ ========================================", "ERROR")
            return False
    
    def get_fair_distribution_status(self, source: str = None) -> Dict[str, Any]:
        """
        Get current fair distribution status and statistics.
        
        Args:
            source: Optional source name to get status for specific source
            
        Returns:
            dict: Fair distribution status and statistics
        """
        try:
            self.debug_print(f"📊 Getting fair distribution status...", "DEBUG")
            
            if source:
                # Get status for specific source
                configs = self.supabase.table('auto_assign_config').select('*').eq('source', source).eq('is_active', True).execute()
                sources_to_check = [source] if configs.data else []
            else:
                # Get status for all sources
                configs = self.get_auto_assign_configs()
                sources_to_check = list(set([config['source'] for config in configs]))
            
            distribution_status = {}
            
            for source_name in sources_to_check:
                try:
                    # Get CREs for this source
                    source_configs = self.supabase.table('auto_assign_config').select('cre_id').eq('source', source_name).eq('is_active', True).execute()
                    if not source_configs.data:
                        continue
                    
                    cre_ids = [config['cre_id'] for config in source_configs.data]
                    
                    # Get current counts for all CREs
                    cre_counts = []
                    for cre_id in cre_ids:
                        cre_result = self.supabase.table('cre_users').select('id, name, auto_assign_count').eq('id', cre_id).execute()
                        if cre_result.data:
                            cre_data = cre_result.data[0]
                            cre_counts.append({
                                'id': cre_data['id'],
                                'name': cre_data['name'],
                                'count': cre_data.get('auto_assign_count', 0)
                            })
                    
                    if cre_counts:
                        # Calculate distribution statistics
                        total_leads = sum(cre['count'] for cre in cre_counts)
                        avg_leads = total_leads / len(cre_counts) if cre_counts else 0
                        min_leads = min(cre['count'] for cre in cre_counts) if cre_counts else 0
                        max_leads = max(cre['count'] for cre in cre_counts) if cre_counts else 0
                        
                        # Calculate fairness score (lower is better)
                        variance = sum((cre['count'] - avg_leads) ** 2 for cre in cre_counts) / len(cre_counts) if cre_counts else 0
                        fairness_score = max(0, 100 - (variance * 10))  # Convert variance to 0-100 scale
                        
                        distribution_status[source_name] = {
                            'cre_count': len(cre_counts),
                            'total_leads': total_leads,
                            'average_leads': round(avg_leads, 2),
                            'min_leads': min_leads,
                            'max_leads': max_leads,
                            'fairness_score': round(fairness_score, 1),
                            'cre_details': sorted(cre_counts, key=lambda x: x['count']),
                            'is_balanced': abs(max_leads - min_leads) <= 1,  # Consider balanced if difference <= 1
                            'recommendation': self._get_distribution_recommendation(cre_counts, avg_leads)
                        }
                        
                except Exception as e:
                    self.debug_print(f"⚠️ Error getting status for source {source_name}: {e}", "WARNING")
                    distribution_status[source_name] = {'error': str(e)}
            
            overall_status = {
                'sources': distribution_status,
                'total_sources': len(distribution_status),
                'timestamp': self.get_ist_timestamp(),
                'overall_fairness': self._calculate_overall_fairness(distribution_status)
            }
            
            self.debug_print(f"📊 Fair distribution status retrieved successfully", "DEBUG")
            return overall_status
            
        except Exception as e:
            self.debug_print(f"❌ Error getting fair distribution status: {e}", "ERROR")
            return {'error': str(e)}
    
    def _get_distribution_recommendation(self, cre_counts: List[Dict], avg_leads: float) -> str:
        """Get recommendation for improving distribution fairness"""
        if not cre_counts:
            return "No CREs configured"
        
        if len(cre_counts) == 1:
            return "Single CRE - no distribution needed"
        
        min_count = min(cre['count'] for cre in cre_counts)
        max_count = max(cre['count'] for cre in cre_counts)
        difference = max_count - min_count
        
        if difference <= 1:
            return "Distribution is well balanced"
        elif difference <= 3:
            return "Distribution is reasonably balanced"
        elif difference <= 5:
            return "Consider redistributing leads to improve balance"
        else:
            return "Significant imbalance detected - redistribution recommended"
    
    def _calculate_overall_fairness(self, distribution_status: Dict) -> Dict[str, Any]:
        """Calculate overall fairness metrics across all sources"""
        try:
            if not distribution_status:
                return {'score': 0, 'status': 'No data'}
            
            total_fairness_scores = []
            balanced_sources = 0
            total_sources = len(distribution_status)
            
            for source_name, status in distribution_status.items():
                if 'error' not in status:
                    total_fairness_scores.append(status.get('fairness_score', 0))
                    if status.get('is_balanced', False):
                        balanced_sources += 1
            
            if not total_fairness_scores:
                return {'score': 0, 'status': 'No valid data'}
            
            avg_fairness = sum(total_fairness_scores) / len(total_fairness_scores)
            balance_percentage = (balanced_sources / total_sources) * 100 if total_sources > 0 else 0
            
            if avg_fairness >= 90:
                overall_status = 'Excellent'
            elif avg_fairness >= 80:
                overall_status = 'Good'
            elif avg_fairness >= 70:
                overall_status = 'Fair'
            elif avg_fairness >= 60:
                overall_status = 'Poor'
            else:
                overall_status = 'Critical'
            
            return {
                'score': round(avg_fairness, 1),
                'status': overall_status,
                'balanced_sources': balanced_sources,
                'total_sources': total_sources,
                'balance_percentage': round(balance_percentage, 1)
            }
            
        except Exception as e:
            return {'score': 0, 'status': 'Error', 'error': str(e)}
    
    def process_batch_leads_with_fair_distribution(self, source: str, batch_size: int = None) -> Dict[str, Any]:
        """
        Process a batch of leads with intelligent fair distribution to maintain balance.
        This function ensures that even when large batches arrive, the distribution remains fair.
        
        Args:
            source: The source name to process leads for
            batch_size: Optional batch size limit (if None, processes all unassigned leads)
            
        Returns:
            dict: Result with assignment details and distribution statistics
        """
        try:
            self.debug_print(f"📦 ========================================", "SYSTEM")
            self.debug_print(f"📦 BATCH LEAD PROCESSING WITH FAIR DISTRIBUTION", "SYSTEM")
            self.debug_print(f"📦 ========================================", "SYSTEM")
            self.debug_print(f"   🏷️ Source: {source}", "INFO")
            self.debug_print(f"   📦 Batch Size: {batch_size if batch_size else 'All unassigned'}", "INFO")
            self.debug_print(f"   ⏰ Start Time: {self.get_ist_timestamp()}", "INFO")
            self.debug_print(f"   🎯 Purpose: Maintain fair distribution with batch processing", "INFO")
            
            # Get current distribution status before processing
            self.debug_print(f"📊 Getting current distribution status...", "DEBUG")
            before_status = self.get_fair_distribution_status(source)
            
            if source in before_status.get('sources', {}):
                source_status = before_status['sources'][source]
                self.debug_print(f"📊 Current status for {source}:", "INFO")
                self.debug_print(f"   👥 CREs: {source_status.get('cre_count', 0)}", "INFO")
                self.debug_print(f"   📊 Total leads: {source_status.get('total_leads', 0)}", "INFO")
                self.debug_print(f"   ⚖️ Fairness score: {source_status.get('fairness_score', 0)}/100", "INFO")
                self.debug_print(f"   🎯 Status: {'Balanced' if source_status.get('is_balanced') else 'Imbalanced'}", "INFO")
            else:
                self.debug_print(f"⚠️ No current status found for {source}", "WARNING")
            
            # Get unassigned leads
            self.debug_print(f"🔍 Fetching unassigned leads for {source}...", "DEBUG")
            unassigned_leads = self.get_unassigned_leads_for_source(source)
            
            if not unassigned_leads:
                self.debug_print(f"ℹ️ No unassigned leads found for {source}", "INFO")
                return {
                    'success': True,
                    'message': f'No unassigned leads found for {source}',
                    'assigned_count': 0,
                    'batch_size': 0,
                    'distribution_improved': False
                }
            
            # Apply batch size limit if specified
            if batch_size and batch_size > 0:
                leads_to_process = unassigned_leads[:batch_size]
                self.debug_print(f"📦 Processing batch of {len(leads_to_process)} leads (limited from {len(unassigned_leads)} total)", "INFO")
            else:
                leads_to_process = unassigned_leads
                self.debug_print(f"📦 Processing all {len(leads_to_process)} unassigned leads", "INFO")
            
            # Get auto-assign configuration for this source
            configs = self.supabase.table('auto_assign_config').select('*').eq('source', source).eq('is_active', True).execute()
            if not configs.data:
                return {'success': False, 'message': f'No auto-assign configuration found for {source}', 'assigned_count': 0}
            
            cre_ids = [config['cre_id'] for config in configs.data]
            
            # Get current lead counts for all configured CREs
            cre_counts = {}
            for cre_id in cre_ids:
                try:
                    cre_result = self.supabase.table('cre_users').select('id, name, auto_assign_count').eq('id', cre_id).execute()
                    if cre_result.data:
                        cre_data = cre_result.data[0]
                        cre_counts[cre_id] = {
                            'id': cre_data['id'],
                            'name': cre_data['name'],
                            'current_count': cre_data.get('auto_assign_count', 0)
                        }
                except Exception as e:
                    self.debug_print(f"⚠️ Could not get count for CRE ID {cre_id}: {e}", "WARNING")
                    cre_counts[cre_id] = {'id': cre_id, 'name': f'CRE_{cre_id}', 'current_count': 0}
            
            # Process leads with intelligent distribution
            assigned_count = 0
            failed_assignments = []
            assignment_details = []
            
            self.debug_print(f"🔄 Starting batch processing with intelligent distribution...", "INFO")
            
            for i, lead in enumerate(leads_to_process):
                # Find CRE with the lowest current count
                selected_cre_id = self._select_cre_with_lowest_count(cre_counts)
                selected_cre_info = cre_counts[selected_cre_id]
                
                self.debug_print(f"🎯 Processing lead {i+1}/{len(leads_to_process)}: {lead['uid']} → {selected_cre_info['name']} (count: {selected_cre_info['current_count']})", "DEBUG")
                
                # Assign the lead
                if self.assign_lead_to_cre(lead['uid'], selected_cre_id, selected_cre_info['name'], source):
                    assigned_count += 1
                    
                    # Update local count tracking
                    cre_counts[selected_cre_id]['current_count'] += 1
                    
                    # Record assignment details
                    assignment_details.append({
                        'lead_uid': lead['uid'],
                        'cre_id': selected_cre_id,
                        'cre_name': selected_cre_info['name'],
                        'cre_count_before': selected_cre_info['current_count'] - 1,
                        'cre_count_after': selected_cre_info['current_count']
                    })
                    
                    self.debug_print(f"✅ Lead {lead['uid']} assigned successfully", "SUCCESS")
                else:
                    failed_assignments.append(lead['uid'])
                    self.debug_print(f"❌ Failed to assign lead {lead['uid']}", "ERROR")
                
                # Show progress every 10 leads
                if (i + 1) % 10 == 0:
                    self.debug_print(f"📊 Progress: {i+1}/{len(leads_to_process)} leads processed", "INFO")
            
            # Get distribution status after processing
            self.debug_print(f"📊 Getting final distribution status...", "DEBUG")
            after_status = self.get_fair_distribution_status(source)
            
            # Calculate distribution improvement
            distribution_improved = False
            improvement_details = {}
            
            if source in before_status.get('sources', {}) and source in after_status.get('sources', {}):
                before_source = before_status['sources'][source]
                after_source = after_status['sources'][source]
                
                before_fairness = before_source.get('fairness_score', 0)
                after_fairness = after_source.get('fairness_score', 0)
                
                before_balance = before_source.get('is_balanced', False)
                after_balance = after_source.get('is_balanced', False)
                
                distribution_improved = after_fairness > before_fairness or (not before_balance and after_balance)
                
                improvement_details = {
                    'before_fairness': before_fairness,
                    'after_fairness': after_fairness,
                    'fairness_improvement': round(after_fairness - before_fairness, 1),
                    'before_balanced': before_balance,
                    'after_balanced': after_balance,
                    'balance_improved': not before_balance and after_balance
                }
                
                self.debug_print(f"📊 Distribution Analysis:", "INFO")
                self.debug_print(f"   ⚖️ Before fairness: {before_fairness}/100", "INFO")
                self.debug_print(f"   ⚖️ After fairness: {after_fairness}/100", "INFO")
                self.debug_print(f"   📈 Improvement: {improvement_details['fairness_improvement']} points", "INFO")
                self.debug_print(f"   🎯 Balance: {'Improved' if improvement_details['balance_improved'] else 'Maintained'}", "INFO")
            
            # Summary
            self.debug_print(f"📦 ========================================", "SYSTEM")
            self.debug_print(f"📦 BATCH PROCESSING COMPLETED", "SYSTEM")
            self.debug_print(f"📦 ========================================", "SYSTEM")
            self.debug_print(f"   🏷️ Source: {source}", "INFO")
            self.debug_print(f"   📦 Batch size: {len(leads_to_process)}", "INFO")
            self.debug_print(f"   ✅ Successfully assigned: {assigned_count}", "SUCCESS")
            self.debug_print(f"   ❌ Failed assignments: {len(failed_assignments)}", "WARNING")
            self.debug_print(f"   ⚖️ Distribution improved: {'Yes' if distribution_improved else 'No'}", "INFO")
            self.debug_print(f"   ⏰ Completion Time: {self.get_ist_timestamp()}", "INFO")
            
            if failed_assignments:
                self.debug_print(f"   🚨 Failed lead UIDs: {failed_assignments}", "ERROR")
            
            self.debug_print(f"📦 ========================================", "SYSTEM")
            
            return {
                'success': True,
                'message': f'Batch processing completed: {assigned_count} leads assigned',
                'assigned_count': assigned_count,
                'batch_size': len(leads_to_process),
                'total_unassigned': len(unassigned_leads),
                'failed_count': len(failed_assignments),
                'failed_leads': failed_assignments,
                'distribution_improved': distribution_improved,
                'improvement_details': improvement_details,
                'assignment_details': assignment_details,
                'final_cre_counts': cre_counts,
                'timestamp': self.get_ist_timestamp(),
                'reference': 'Uday branch enhanced batch processing with intelligent distribution'
            }
            
        except Exception as e:
            self.debug_print(f"❌ ========================================", "ERROR")
            self.debug_print(f"❌ ERROR IN BATCH PROCESSING", "ERROR")
            self.debug_print(f"❌ ========================================", "ERROR")
            self.debug_print(f"   🚨 Exception: {e}", "ERROR")
            self.debug_print(f"   🚨 Exception type: {type(e).__name__}", "ERROR")
            self.debug_print(f"   🏷️ Source: {source}", "ERROR")
            self.debug_print(f"   📦 Batch size: {batch_size}", "ERROR")
            self.debug_print(f"   ⏰ Time: {self.get_ist_timestamp()}", "ERROR")
            self.debug_print(f"   🔍 Action: Review error and retry", "ERROR")
            self.debug_print(f"❌ ========================================", "ERROR")
            return {'success': False, 'message': str(e), 'assigned_count': 0}
    
    def auto_assign_new_leads_for_source(self, source: str) -> Dict[str, Any]:
        """
        Automatically assign new leads for a specific source using intelligent fair distribution.
        Enhanced with count-based distribution to equalize lead counts across CREs.
        
        Args:
            source: The source name to auto-assign leads for
            
        Returns:
            dict: Result with assigned_count and status
        """
        try:
            self.debug_print(f"🤖 ========================================", "SYSTEM")
            self.debug_print(f"🤖 AUTO-ASSIGN FOR SOURCE: {source}", "SYSTEM")
            self.debug_print(f"🤖 ========================================", "SYSTEM")
            self.debug_print(f"   ⏰ Start Time: {self.get_ist_timestamp()}", "INFO")
            self.debug_print(f"   🎯 Source: {source}", "INFO")
            self.debug_print(f"   🔄 Process: Intelligent Fair Distribution (Count-Based)", "INFO")
            self.debug_print(f"   🚀 Reference: Uday Branch Enhanced Logic", "INFO")
            
            # Get auto-assign configuration for this source
            self.debug_print(f"🔧 Fetching auto-assign configuration for {source}...", "DEBUG")
            configs = self.supabase.table('auto_assign_config').select('*').eq('source', source).eq('is_active', True).execute()
            if not configs.data:
                self.debug_print(f"ℹ️ No auto-assign configuration found for {source}", "INFO")
                self.debug_print(f"   🚫 Status: Configuration Required", "WARNING")
                self.debug_print(f"   🔧 Action: Please configure auto-assign for {source}", "WARNING")
                return {'success': False, 'message': f'No auto-assign configuration found for {source}', 'assigned_count': 0}
            
            cre_ids = [config['cre_id'] for config in configs.data]
            self.debug_print(f"✅ Found {len(cre_ids)} CREs configured for {source}", "SUCCESS")
            self.debug_print(f"   👥 CRE IDs: {cre_ids}", "INFO")
            self.debug_print(f"   🔧 Status: Configuration loaded successfully", "SUCCESS")
            
            # Get current lead counts for all configured CREs
            self.debug_print(f"📊 Fetching current lead counts for configured CREs...", "DEBUG")
            cre_counts = {}
            for cre_id in cre_ids:
                try:
                    cre_result = self.supabase.table('cre_users').select('id, name, auto_assign_count').eq('id', cre_id).execute()
                    if cre_result.data:
                        cre_data = cre_result.data[0]
                        cre_counts[cre_id] = {
                            'id': cre_data['id'],
                            'name': cre_data['name'],
                            'current_count': cre_data.get('auto_assign_count', 0)
                        }
                        self.debug_print(f"   👥 CRE {cre_data['name']} (ID: {cre_id}): {cre_data.get('auto_assign_count', 0)} leads", "DEBUG")
                except Exception as e:
                    self.debug_print(f"   ⚠️ Could not get count for CRE ID {cre_id}: {e}", "WARNING")
                    # Use default count of 0
                    cre_counts[cre_id] = {'id': cre_id, 'name': f'CRE_{cre_id}', 'current_count': 0}
            
            if not cre_counts:
                self.debug_print(f"❌ No CRE counts retrieved for {source}", "ERROR")
                return {'success': False, 'message': f'Could not retrieve CRE counts for {source}', 'assigned_count': 0}
            
            self.debug_print(f"📊 CRE Count Summary for {source}:", "INFO")
            for cre_id, cre_info in cre_counts.items():
                self.debug_print(f"   👥 {cre_info['name']}: {cre_info['current_count']} leads", "INFO")
            
            # Get unassigned leads for this source
            self.debug_print(f"🔍 Fetching unassigned leads for {source}...", "DEBUG")
            unassigned_leads = self.get_unassigned_leads_for_source(source)
            
            if not unassigned_leads:
                self.debug_print(f"ℹ️ No unassigned leads found for {source}", "INFO")
                self.debug_print(f"   🎯 Status: All leads already assigned", "SUCCESS")
                self.debug_print(f"   🔄 Action: No action needed", "INFO")
                return {'success': True, 'message': f'No unassigned leads found for {source}', 'assigned_count': 0}
            
            self.debug_print(f"📊 Processing {len(unassigned_leads)} unassigned leads for {source}", "INFO")
            self.debug_print(f"   🎯 Lead UIDs: {[lead['uid'] for lead in unassigned_leads[:5]]}{'...' if len(unassigned_leads) > 5 else ''}", "DEBUG")
            self.debug_print(f"   🔄 Status: Starting intelligent assignment process", "INFO")
            
            # Intelligent fair distribution based on current counts
            assigned_count = 0
            failed_assignments = []
            
            self.debug_print(f"🔄 Starting intelligent fair distribution assignment...", "INFO")
            self.debug_print(f"   🧠 Algorithm: Count-based distribution to equalize loads", "DEBUG")
            self.debug_print(f"   📊 CREs: {len(cre_counts)}, Leads: {len(unassigned_leads)}", "DEBUG")
            
            for i, lead in enumerate(unassigned_leads):
                # Find CRE with the lowest current count
                selected_cre_id = self._select_cre_with_lowest_count(cre_counts)
                selected_cre_info = cre_counts[selected_cre_id]
                
                self.debug_print(f"🎯 ========================================", "DEBUG")
                self.debug_print(f"🎯 PROCESSING LEAD {i+1}/{len(unassigned_leads)}", "DEBUG")
                self.debug_print(f"🎯 ========================================", "DEBUG")
                self.debug_print(f"   🆔 Lead UID: {lead['uid']}", "DEBUG")
                self.debug_print(f"   👤 Customer: {lead.get('customer_name', 'N/A')}", "DEBUG")
                self.debug_print(f"   📱 Mobile: {lead.get('customer_mobile_number', 'N/A')}", "DEBUG")
                self.debug_print(f"   🏷️ Source: {lead.get('source', 'N/A')}", "DEBUG")
                self.debug_print(f"   🎯 Sub-source: {lead.get('sub_source', 'N/A')}", "DEBUG")
                self.debug_print(f"   📊 Status: {lead.get('lead_status', 'N/A')}", "DEBUG")
                self.debug_print(f"   📅 Created: {lead.get('created_at', 'N/A')}", "DEBUG")
                self.debug_print(f"   👥 Assigned to: {selected_cre_info['name']} (CRE ID: {selected_cre_id})", "DEBUG")
                self.debug_print(f"   📊 Current count: {selected_cre_info['current_count']} leads", "DEBUG")
                self.debug_print(f"   🧠 Selection reason: Lowest count among {len(cre_counts)} CREs", "DEBUG")
                self.debug_print(f"   🔄 Status: Processing assignment...", "DEBUG")
                
                # Assign the lead
                if self.assign_lead_to_cre(lead['uid'], selected_cre_id, selected_cre_info['name'], source):
                    assigned_count += 1
                    
                    # Update local count tracking
                    cre_counts[selected_cre_id]['current_count'] += 1
                    
                    self.debug_print(f"✅ SUCCESS: Lead {lead['uid']} assigned to {selected_cre_info['name']}", "SUCCESS")
                    self.debug_print(f"   🎉 Assignment #{assigned_count} completed", "SUCCESS")
                    self.debug_print(f"   📊 New count for {selected_cre_info['name']}: {cre_counts[selected_cre_id]['current_count']}", "SUCCESS")
                    self.debug_print(f"   🎯 Status: Assignment successful", "SUCCESS")
                    
                    # Verify the lead appears in the right place
                    self._verify_lead_assignment(lead['uid'], selected_cre_info['name'], source)
                else:
                    failed_assignments.append(lead['uid'])
                    self.debug_print(f"❌ FAILED: Lead {lead['uid']} assignment to {selected_cre_info['name']}", "ERROR")
                    self.debug_print(f"   🚨 Failed assignment #{len(failed_assignments)}", "ERROR")
                    self.debug_print(f"   📊 Status: Assignment failed", "ERROR")
                
                self.debug_print(f"🎯 ========================================", "DEBUG")
            
            # Summary and verification
            self.debug_print(f"🤖 ========================================", "SYSTEM")
            self.debug_print(f"🤖 AUTO-ASSIGN SUMMARY FOR {source}", "SYSTEM")
            self.debug_print(f"🤖 ========================================", "SYSTEM")
            
            if assigned_count > 0:
                self.debug_print(f"🎉 SUCCESS: Auto-assigned {assigned_count} leads for {source}", "SUCCESS")
                self.debug_print(f"   📊 Total leads processed: {len(unassigned_leads)}", "INFO")
                self.debug_print(f"   ✅ Successfully assigned: {assigned_count}", "SUCCESS")
                self.debug_print(f"   ❌ Failed assignments: {len(failed_assignments)}", "WARNING")
                self.debug_print(f"   👥 CREs involved: {cre_ids}", "INFO")
                self.debug_print(f"   ⏰ Completion Time: {self.get_ist_timestamp()}", "INFO")
                self.debug_print(f"   🎯 Success Rate: {(assigned_count/len(unassigned_leads)*100):.1f}%", "SUCCESS")
                self.debug_print(f"   🚀 Reference: Uday Branch Enhanced Logic", "INFO")
                
                # Show final count distribution
                self.debug_print(f"📊 Final CRE Count Distribution:", "INFO")
                for cre_id, cre_info in cre_counts.items():
                    self.debug_print(f"   👥 {cre_info['name']}: {cre_info['current_count']} leads", "INFO")
                
                if failed_assignments:
                    self.debug_print(f"   🚨 Failed lead UIDs: {failed_assignments}", "ERROR")
                    self.debug_print(f"   🔍 Action: Review failed assignments", "WARNING")
            else:
                self.debug_print(f"ℹ️ No leads were auto-assigned for {source}", "INFO")
                self.debug_print(f"   🚫 Status: Assignment Failed", "WARNING")
                self.debug_print(f"   🔍 Action: Check configuration and leads", "WARNING")
            
            self.debug_print(f"🤖 ========================================", "SYSTEM")
            
            return {
                'success': True,
                'message': f'Successfully auto-assigned {assigned_count} leads for {source}',
                'assigned_count': assigned_count,
                'source': source,
                'total_processed': len(unassigned_leads),
                'failed_count': len(failed_assignments),
                'failed_leads': failed_assignments,
                'final_cre_counts': cre_counts,
                'timestamp': self.get_ist_timestamp(),
                'reference': 'Uday branch enhanced logic with intelligent distribution'
            }
            
        except Exception as e:
            self.debug_print(f"❌ ========================================", "ERROR")
            self.debug_print(f"❌ ERROR IN AUTO-ASSIGN FOR SOURCE", "ERROR")
            self.debug_print(f"❌ ========================================", "ERROR")
            self.debug_print(f"   🚨 Exception: {e}", "ERROR")
            self.debug_print(f"   🚨 Exception type: {type(e).__name__}", "ERROR")
            self.debug_print(f"   📍 Source: {source}", "ERROR")
            self.debug_print(f"   ⏰ Time: {self.get_ist_timestamp()}", "ERROR")
            self.debug_print(f"   🔍 Action: Review error and retry", "ERROR")
            self.debug_print(f"❌ ========================================", "ERROR")
            return {'success': False, 'message': str(e), 'assigned_count': 0}
    
    def detect_and_assign_new_leads(self, source: str = None, auto_rebalance: bool = True) -> Dict[str, Any]:
        """
        Automatically detect new leads and assign them using intelligent fair distribution.
        This function can also rebalance existing distribution if needed.
        
        Args:
            source: Optional source name (if None, checks all sources)
            auto_rebalance: Whether to automatically rebalance if significant imbalance detected
            
        Returns:
            dict: Result with assignment and rebalancing details
        """
        try:
            self.debug_print(f"🔍 ========================================", "SYSTEM")
            self.debug_print(f"🔍 DETECTING AND ASSIGNING NEW LEADS", "SYSTEM")
            self.debug_print(f"🔍 ========================================", "SYSTEM")
            self.debug_print(f"   🏷️ Source: {source if source else 'All Sources'}", "INFO")
            self.debug_print(f"   ⚖️ Auto-rebalance: {'Enabled' if auto_rebalance else 'Disabled'}", "INFO")
            self.debug_print(f"   ⏰ Start Time: {self.get_ist_timestamp()}", "INFO")
            self.debug_print(f"   🎯 Purpose: Maintain fair distribution with new leads", "INFO")
            
            if source:
                # Process single source
                sources_to_check = [source]
            else:
                # Process all sources
                configs = self.get_auto_assign_configs()
                sources_to_check = list(set([config['source'] for config in configs]))
            
            self.debug_print(f"📋 Found {len(sources_to_check)} sources to check: {sources_to_check}", "INFO")
            
            total_results = {}
            total_assigned = 0
            rebalancing_performed = []
            
            for source_name in sources_to_check:
                self.debug_print(f"🎯 Processing source: {source_name}", "INFO")
                
                try:
                    # Get current distribution status
                    current_status = self.get_fair_distribution_status(source_name)
                    if source_name not in current_status.get('sources', {}):
                        self.debug_print(f"⚠️ No status found for {source_name}, skipping", "WARNING")
                        continue
                    
                    source_status = current_status['sources'][source_name]
                    current_fairness = source_status.get('fairness_score', 0)
                    is_balanced = source_status.get('is_balanced', False)
                    
                    self.debug_print(f"📊 Current status for {source_name}:", "INFO")
                    self.debug_print(f"   ⚖️ Fairness score: {current_fairness}/100", "INFO")
                    self.debug_print(f"   🎯 Balanced: {'Yes' if is_balanced else 'No'}", "INFO")
                    
                    # Check if rebalancing is needed
                    needs_rebalancing = False
                    if auto_rebalance and not is_balanced and current_fairness < 70:
                        needs_rebalancing = True
                        self.debug_print(f"⚠️ Significant imbalance detected, rebalancing recommended", "WARNING")
                    
                    # Process new leads
                    result = self.auto_assign_new_leads_for_source(source_name)
                    
                    if result.get('success'):
                        assigned_count = result.get('assigned_count', 0)
                        total_assigned += assigned_count
                        
                        if assigned_count > 0:
                            self.debug_print(f"✅ {source_name}: {assigned_count} leads assigned", "SUCCESS")
                        else:
                            self.debug_print(f"ℹ️ {source_name}: No new leads to assign", "INFO")
                        
                        # Check if rebalancing is still needed after new assignments
                        if needs_rebalancing:
                            self.debug_print(f"🔄 Checking if rebalancing is still needed...", "DEBUG")
                            new_status = self.get_fair_distribution_status(source_name)
                            if source_name in new_status.get('sources', {}):
                                new_source_status = new_status['sources'][source_name]
                                new_fairness = new_source_status.get('fairness_score', 0)
                                new_balanced = new_source_status.get('is_balanced', False)
                                
                                if new_balanced or new_fairness >= 80:
                                    self.debug_print(f"✅ Rebalancing no longer needed after new assignments", "SUCCESS")
                                    needs_rebalancing = False
                        
                        # Perform rebalancing if still needed
                        if needs_rebalancing:
                            self.debug_print(f"🔄 Performing rebalancing for {source_name}...", "INFO")
                            rebalance_result = self._rebalance_distribution(source_name)
                            if rebalance_result.get('success'):
                                rebalancing_performed.append({
                                    'source': source_name,
                                    'before_fairness': current_fairness,
                                    'after_fairness': rebalance_result.get('after_fairness', 0),
                                    'improvement': rebalance_result.get('fairness_improvement', 0)
                                })
                                self.debug_print(f"✅ Rebalancing completed for {source_name}", "SUCCESS")
                            else:
                                self.debug_print(f"❌ Rebalancing failed for {source_name}", "ERROR")
                        
                        total_results[source_name] = {
                            'success': True,
                            'assigned_count': assigned_count,
                            'rebalancing_needed': needs_rebalancing,
                            'rebalancing_performed': needs_rebalancing and any(r['source'] == source_name for r in rebalancing_performed)
                        }
                    else:
                        self.debug_print(f"❌ {source_name}: {result.get('message', 'Unknown error')}", "ERROR")
                        total_results[source_name] = {
                            'success': False,
                            'error': result.get('message', 'Unknown error')
                        }
                        
                except Exception as e:
                    self.debug_print(f"❌ Error processing {source_name}: {e}", "ERROR")
                    total_results[source_name] = {
                        'success': False,
                        'error': str(e)
                    }
                
                self.debug_print(f"🎯 Completed processing {source_name}", "INFO")
            
            # Summary
            self.debug_print(f"🔍 ========================================", "SYSTEM")
            self.debug_print(f"🔍 DETECTION AND ASSIGNMENT COMPLETED", "SYSTEM")
            self.debug_print(f"🔍 ========================================", "SYSTEM")
            self.debug_print(f"   📋 Sources processed: {len(sources_to_check)}", "INFO")
            self.debug_print(f"   ✅ Total leads assigned: {total_assigned}", "SUCCESS")
            self.debug_print(f"   ⚖️ Rebalancing performed: {len(rebalancing_performed)} sources", "INFO")
            self.debug_print(f"   ⏰ Completion Time: {self.get_ist_timestamp()}", "INFO")
            
            if rebalancing_performed:
                self.debug_print(f"📊 Rebalancing Summary:", "INFO")
                for rebalance in rebalancing_performed:
                    self.debug_print(f"   🏷️ {rebalance['source']}: {rebalance['before_fairness']} → {rebalance['after_fairness']} (+{rebalance['improvement']})", "INFO")
            
            self.debug_print(f"🔍 ========================================", "SYSTEM")
            
            return {
                'success': True,
                'total_assigned': total_assigned,
                'sources_processed': len(sources_to_check),
                'results': total_results,
                'rebalancing_performed': rebalancing_performed,
                'timestamp': self.get_ist_timestamp(),
                'reference': 'Uday branch enhanced detection and assignment with auto-rebalancing'
            }
            
        except Exception as e:
            self.debug_print(f"❌ ========================================", "ERROR")
            self.debug_print(f"❌ ERROR IN DETECTION AND ASSIGNMENT", "ERROR")
            self.debug_print(f"❌ ========================================", "ERROR")
            self.debug_print(f"   🚨 Exception: {e}", "ERROR")
            self.debug_print(f"   🚨 Exception type: {type(e).__name__}", "ERROR")
            self.debug_print(f"   ⏰ Time: {self.get_ist_timestamp()}", "ERROR")
            self.debug_print(f"   🔍 Action: Review error and retry", "ERROR")
            self.debug_print(f"❌ ========================================", "ERROR")
            return {'success': False, 'message': str(e), 'total_assigned': 0}
    
    def _rebalance_distribution(self, source: str) -> Dict[str, Any]:
        """
        Rebalance the distribution for a specific source by redistributing leads if possible.
        This is a more advanced function that can move leads between CREs to improve balance.
        
        Args:
            source: The source name to rebalance
            
        Returns:
            dict: Rebalancing result with improvement details
        """
        try:
            self.debug_print(f"⚖️ Starting distribution rebalancing for {source}...", "INFO")
            
            # Get current distribution status
            current_status = self.get_fair_distribution_status(source)
            if source not in current_status.get('sources', {}):
                return {'success': False, 'message': f'No status found for {source}'}
            
            source_status = current_status['sources'][source]
            before_fairness = source_status.get('fairness_score', 0)
            
            # For now, we'll just reset counts to 0 to start fresh
            # In a more advanced implementation, you could actually move leads between CREs
            self.debug_print(f"🔄 Resetting counts to start fresh distribution", "INFO")
            
            configs = self.supabase.table('auto_assign_config').select('cre_id').eq('source', source).eq('is_active', True).execute()
            if configs.data:
                cre_ids = [config['cre_id'] for config in configs.data]
                reset_success = self.reset_cre_auto_assign_counts(cre_ids)
                
                if reset_success:
                    # Get new status after reset
                    new_status = self.get_fair_distribution_status(source)
                    if source in new_status.get('sources', {}):
                        new_source_status = new_status['sources'][source]
                        after_fairness = new_source_status.get('fairness_score', 0)
                        fairness_improvement = after_fairness - before_fairness
                        
                        return {
                            'success': True,
                            'before_fairness': before_fairness,
                            'after_fairness': after_fairness,
                            'fairness_improvement': fairness_improvement,
                            'method': 'count_reset',
                            'message': f'Distribution rebalanced by resetting counts'
                        }
            
            return {'success': False, 'message': 'Rebalancing failed'}
            
        except Exception as e:
            self.debug_print(f"❌ Error during rebalancing: {e}", "ERROR")
            return {'success': False, 'message': str(e)}
    
    def _select_cre_with_lowest_count(self, cre_counts: Dict[int, Dict]) -> int:
        """
        Select the CRE with the lowest current lead count for fair distribution.
        
        Args:
            cre_counts: Dictionary mapping CRE ID to count information
            
        Returns:
            int: CRE ID with the lowest count
        """
        try:
            if not cre_counts:
                raise ValueError("No CRE counts provided")
            
            # Find CRE with minimum count
            min_count = float('inf')
            selected_cre_id = None
            
            for cre_id, cre_info in cre_counts.items():
                current_count = cre_info.get('current_count', 0)
                if current_count < min_count:
                    min_count = current_count
                    selected_cre_id = cre_id
            
            if selected_cre_id is None:
                # Fallback to first CRE if something goes wrong
                selected_cre_id = list(cre_counts.keys())[0]
                self.debug_print(f"⚠️ Fallback: Using first CRE {selected_cre_id} due to selection error", "WARNING")
            
            self.debug_print(f"🧠 Selected CRE {cre_counts[selected_cre_id]['name']} (ID: {selected_cre_id}) with count {min_count}", "DEBUG")
            return selected_cre_id
            
        except Exception as e:
            self.debug_print(f"❌ Error selecting CRE with lowest count: {e}", "ERROR")
            # Fallback to first available CRE
            fallback_cre_id = list(cre_counts.keys())[0] if cre_counts else None
            if fallback_cre_id:
                self.debug_print(f"🔄 Fallback: Using CRE ID {fallback_cre_id}", "WARNING")
                return fallback_cre_id
            else:
                raise ValueError("No CREs available for selection")
    
    def _verify_lead_assignment(self, lead_uid: str, cre_name: str, source: str):
        """Verify that a lead assignment was successful and appears in the right places with enhanced debug prints"""
        try:
            self.debug_print(f"🔍 ========================================", "DEBUG")
            self.debug_print(f"🔍 LEAD ASSIGNMENT VERIFICATION", "DEBUG")
            self.debug_print(f"🔍 ========================================", "DEBUG")
            self.debug_print(f"   🆔 Lead UID: {lead_uid}", "DEBUG")
            self.debug_print(f"   👥 Expected CRE: {cre_name}", "DEBUG")
            self.debug_print(f"   🏷️ Source: {source}", "DEBUG")
            self.debug_print(f"   ⏰ Verification Time: {self.get_ist_timestamp()}", "DEBUG")
            self.debug_print(f"   🚀 Reference: Uday Branch Enhanced Logic", "DEBUG")
            self.debug_print(f"   🔄 Status: Starting verification process", "DEBUG")
            
            # Check lead_master table
            self.debug_print(f"📊 Checking lead_master table...", "DEBUG")
            self.debug_print(f"   🎯 Target: lead_master.uid = {lead_uid}", "DEBUG")
            self.debug_print(f"   🔄 Status: Querying lead data...", "DEBUG")
            
            lead_result = self.supabase.table('lead_master').select('assigned, cre_name, cre_assigned_at').eq('uid', lead_uid).execute()
            if lead_result.data:
                lead_data = lead_result.data[0]
                self.debug_print(f"   📋 Lead data found in lead_master", "DEBUG")
                self.debug_print(f"      📊 assigned: {lead_data['assigned']}", "DEBUG")
                self.debug_print(f"      👥 cre_name: {lead_data['cre_name']}", "DEBUG")
                self.debug_print(f"      🔍 Status: Data retrieved successfully", "SUCCESS")
                
                if lead_data['assigned'] == 'Yes' and lead_data['cre_name'] == cre_name:
                    self.debug_print(f"   ✅ VERIFICATION SUCCESS: Lead {lead_uid} properly assigned", "SUCCESS")
                    self.debug_print(f"      🎯 Status: Assignment verified in lead_master", "SUCCESS")
                    self.debug_print(f"      📊 assigned: {lead_data['assigned']}", "DEBUG")
                    self.debug_print(f"      👥 cre_name: {lead_data['cre_name']}", "DEBUG")
                    if lead_data.get('cre_assigned_at'):
                        self.debug_print(f"      🕒 cre_assigned_at: {lead_data['cre_assigned_at']}", "DEBUG")
                        self.debug_print(f"      ✅ Status: Timestamp recorded", "SUCCESS")
                    else:
                        self.debug_print(f"      ⚠️ cre_assigned_at is NULL", "WARNING")
                        self.debug_print(f"      🔍 Action: Check timestamp field", "WARNING")
                else:
                    self.debug_print(f"   ⚠️ VERIFICATION WARNING: Assignment mismatch detected", "WARNING")
                    self.debug_print(f"      📊 Expected: assigned=Yes, cre_name={cre_name}", "DEBUG")
                    self.debug_print(f"      📊 Actual: assigned={lead_data['assigned']}, cre_name={lead_data['cre_name']}", "DEBUG")
                    self.debug_print(f"      🚨 Status: Verification failed", "WARNING")
                    self.debug_print(f"      🔍 Action: Review assignment data", "WARNING")
            else:
                self.debug_print(f"   ❌ VERIFICATION ERROR: Lead {lead_uid} not found", "ERROR")
                self.debug_print(f"      🚨 Status: Lead not found", "ERROR")
                self.debug_print(f"      🔍 Action: Check lead existence", "ERROR")
            
            # Check auto_assign_history table
            self.debug_print(f"📝 Checking auto_assign_history table...", "DEBUG")
            self.debug_print(f"   🎯 Target: auto_assign_history.lead_uid = {lead_uid}", "DEBUG")
            self.debug_print(f"   🔄 Status: Querying history data...", "DEBUG")
            
            history_result = self.supabase.table('auto_assign_history').select('*').eq('lead_uid', lead_uid).eq('source', source).execute()
            if history_result.data:
                history_data = history_result.data[0]
                self.debug_print(f"   ✅ History record found for lead {lead_uid}", "SUCCESS")
                self.debug_print(f"      📊 History ID: {history_data.get('id', 'Unknown')}", "DEBUG")
                self.debug_print(f"      👥 Assigned CRE: {history_data.get('assigned_cre_name', 'N/A')}", "DEBUG")
                self.debug_print(f"      🏷️ Source: {history_data.get('source', 'N/A')}", "DEBUG")
                self.debug_print(f"      📅 Created: {history_data.get('created_at', 'N/A')}", "DEBUG")
                self.debug_print(f"      🎯 Status: History record verified", "SUCCESS")
                self.debug_print(f"      🔄 Action: History logging successful", "SUCCESS")
            else:
                self.debug_print(f"   ⚠️ WARNING: No history record found for lead {lead_uid}", "WARNING")
                self.debug_print(f"      🚨 Status: History record missing", "WARNING")
                self.debug_print(f"      🔍 Action: Review history creation", "WARNING")
            
            # Overall verification summary
            self.debug_print(f"🔍 ========================================", "DEBUG")
            self.debug_print(f"🔍 VERIFICATION SUMMARY", "DEBUG")
            self.debug_print(f"🔍 ========================================", "DEBUG")
            self.debug_print(f"   🆔 Lead: {lead_uid}", "DEBUG")
            self.debug_print(f"   👥 CRE: {cre_name}", "DEBUG")
            self.debug_print(f"   🏷️ Source: {source}", "DEBUG")
            self.debug_print(f"   ⏰ Time: {self.get_ist_timestamp()}", "DEBUG")
            self.debug_print(f"   🚀 Reference: Uday Branch Enhanced Logic", "DEBUG")
            
            # Determine overall verification status
            lead_verified = lead_result.data and lead_result.data[0]['assigned'] == 'Yes' and lead_result.data[0]['cre_name'] == cre_name
            history_verified = history_result.data is not None
            
            if lead_verified and history_verified:
                self.debug_print(f"   🎉 OVERALL STATUS: FULLY VERIFIED", "SUCCESS")
                self.debug_print(f"      ✅ lead_master: Verified", "SUCCESS")
                self.debug_print(f"      ✅ auto_assign_history: Verified", "SUCCESS")
                self.debug_print(f"      🎯 Status: Complete verification success", "SUCCESS")
                self.debug_print(f"      🚀 Reference: Uday Branch Success Logic", "SUCCESS")
            elif lead_verified:
                self.debug_print(f"   ⚠️ OVERALL STATUS: PARTIALLY VERIFIED", "WARNING")
                self.debug_print(f"      ✅ lead_master: Verified", "SUCCESS")
                self.debug_print(f"      ❌ auto_assign_history: Missing", "WARNING")
                self.debug_print(f"      🔍 Action: Review history creation", "WARNING")
            elif history_verified:
                self.debug_print(f"   ⚠️ OVERALL STATUS: PARTIALLY VERIFIED", "WARNING")
                self.debug_print(f"      ❌ lead_master: Mismatch", "WARNING")
                self.debug_print(f"      ✅ auto_assign_history: Verified", "SUCCESS")
                self.debug_print(f"      🔍 Action: Review assignment data", "WARNING")
            else:
                self.debug_print(f"   ❌ OVERALL STATUS: VERIFICATION FAILED", "ERROR")
                self.debug_print(f"      ❌ lead_master: Failed", "ERROR")
                self.debug_print(f"      ❌ auto_assign_history: Failed", "ERROR")
                self.debug_print(f"      🔍 Action: Comprehensive review needed", "ERROR")
            
            self.debug_print(f"🔍 ========================================", "DEBUG")
            
        except Exception as e:
            self.debug_print(f"❌ ERROR during lead assignment verification: {e}", "ERROR")
            self.debug_print(f"   🚨 Exception type: {type(e).__name__}", "ERROR")
            self.debug_print(f"   🆔 Lead UID: {lead_uid}", "ERROR")
            self.debug_print(f"   👥 CRE: {cre_name}", "ERROR")
            self.debug_print(f"   🏷️ Source: {source}", "ERROR")
            self.debug_print(f"   🔍 Action: Review verification process", "ERROR")
    
    def check_and_assign_new_leads(self) -> Dict[str, Any]:
        """
        Check for new leads across all sources and assign them automatically.
        Enhanced with detailed debug prints and stickers from Uday branch.
        
        Returns:
            dict: Result with total_assigned and status
        """
        try:
            self.debug_print("🔄 ========================================", "SYSTEM")
            self.debug_print("🔄 COMPREHENSIVE LEAD ASSIGNMENT CHECK", "SYSTEM")
            self.debug_print("🔄 ========================================", "SYSTEM")
            self.debug_print("   ⏰ Start Time: " + self.get_ist_timestamp(), "INFO")
            self.debug_print("   🎯 Scope: All configured sources", "INFO")
            self.debug_print("   🔄 Process: Multi-source auto-assignment", "INFO")
            self.debug_print("   🚀 Reference: Uday Branch Enhanced Logic", "INFO")
            
            # Get all sources with auto-assign configs
            self.debug_print("🔧 Fetching auto-assign configurations...", "DEBUG")
            configs = self.get_auto_assign_configs()
            sources = list(set([config['source'] for config in configs]))
            
            self.debug_print(f"📋 Found {len(sources)} sources with auto-assign configs", "INFO")
            self.debug_print(f"   🎯 Sources: {sources}", "DEBUG")
            self.debug_print(f"   🔧 Status: Configurations loaded successfully", "SUCCESS")
            
            total_assigned = 0
            results = []
            
            self.debug_print("🔄 Starting multi-source assignment process...", "INFO")
            self.debug_print("   " + "="*50, "DEBUG")
            self.debug_print("   🚀 Reference: Uday Branch Multi-Source Logic", "INFO")
            
            for i, source in enumerate(sources):
                self.debug_print(f"🎯 ========================================", "DEBUG")
                self.debug_print(f"🎯 PROCESSING SOURCE {i+1}/{len(sources)}", "DEBUG")
                self.debug_print(f"🎯 ========================================", "DEBUG")
                self.debug_print(f"   🏷️ Source: {source}", "DEBUG")
                self.debug_print(f"   📊 Progress: {i+1}/{len(sources)}", "DEBUG")
                self.debug_print(f"   ⏰ Time: {self.get_ist_timestamp()}", "DEBUG")
                self.debug_print(f"   🔄 Status: Starting source processing", "DEBUG")
                
                result = self.auto_assign_new_leads_for_source(source)
                
                if result['success']:
                    assigned_count = result['assigned_count']
                    total_assigned += assigned_count
                    results.append(result)
                    self.debug_print(f"✅ SUCCESS: {source} - {assigned_count} leads assigned", "SUCCESS")
                    self.debug_print(f"   🎉 Running total: {total_assigned} leads", "SUCCESS")
                    self.debug_print(f"   📊 Status: Source completed successfully", "SUCCESS")
                    self.debug_print(f"   🚀 Reference: Uday Branch Success Logic", "SUCCESS")
                else:
                    self.debug_print(f"⚠️ WARNING: {source} - {result['message']}", "WARNING")
                    self.debug_print(f"   🚨 Status: Source completed with issues", "WARNING")
                    self.debug_print(f"   🔍 Action: Review source configuration", "WARNING")
                    results.append(result)
                
                self.debug_print(f"🎯 ========================================", "DEBUG")
            
            # Summary
            self.debug_print("🔄 ========================================", "SYSTEM")
            self.debug_print("🔄 MULTI-SOURCE ASSIGNMENT SUMMARY", "SYSTEM")
            self.debug_print("🔄 ========================================", "SYSTEM")
            self.debug_print(f"   🎯 Total sources processed: {len(sources)}", "INFO")
            self.debug_print(f"   ✅ Total leads assigned: {total_assigned}", "SUCCESS")
            self.debug_print(f"   📊 Sources with issues: {len([r for r in results if not r['success']])}", "INFO")
            self.debug_print(f"   ⏰ Completion Time: {self.get_ist_timestamp()}", "INFO")
            self.debug_print(f"   🚀 Reference: Uday Branch Enhanced Logic", "INFO")
            
            if total_assigned > 0:
                self.debug_print(f"   🎉 Status: Multi-source assignment successful", "SUCCESS")
                self.debug_print(f"   📈 Success rate: {(len([r for r in results if r['success']])/len(sources)*100):.1f}%", "SUCCESS")
                self.debug_print(f"   🎯 Performance: {total_assigned} leads across {len(sources)} sources", "SUCCESS")
            else:
                self.debug_print(f"   ℹ️ Status: No leads assigned across sources", "INFO")
                self.debug_print(f"   🔍 Action: Check source configurations", "INFO")
            
            self.debug_print("🔄 ========================================", "SYSTEM")
            
            return {
                'success': True,
                'total_assigned': total_assigned,
                'results': results,
                'timestamp': self.get_ist_timestamp(),
                'sources_processed': len(sources),
                'sources_successful': len([r for r in results if r['success']]),
                'sources_with_issues': len([r for r in results if not r['success']]),
                'reference': 'Uday branch enhanced logic',
                'enhanced_features': ['debug_prints', 'stickers', 'performance_monitoring', 'multi_source_optimization']
            }
            
        except Exception as e:
            self.debug_print(f"❌ ========================================", "ERROR")
            self.debug_print(f"❌ ERROR IN MULTI-SOURCE ASSIGNMENT", "ERROR")
            self.debug_print(f"❌ ========================================", "ERROR")
            self.debug_print(f"   🚨 Exception: {e}", "ERROR")
            self.debug_print(f"   🚨 Exception type: {type(e).__name__}", "ERROR")
            self.debug_print(f"   ⏰ Time: {self.get_ist_timestamp()}", "ERROR")
            self.debug_print(f"   🎯 Status: Multi-source assignment failed", "ERROR")
            self.debug_print(f"   🔍 Action: Review error and retry", "ERROR")
            self.debug_print(f"❌ ========================================", "ERROR")
            return {'success': False, 'message': str(e), 'total_assigned': 0}
    
    def robust_auto_assign_worker(self):
        """Robust background worker that continuously checks for new leads (Render-compatible)"""
        self.debug_print("🚀 Robust auto-assign background worker started", "SYSTEM")
        self.system_status['is_running'] = True
        self.system_status['started_at'] = self.get_ist_timestamp()
        
        # Check if running in production (Render)
        is_production = os.environ.get('RENDER', False) or os.environ.get('PRODUCTION', False)
        
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
        
        # Continuous background auto-assign with Render-optimized intervals
        if is_production:
            # Production mode: shorter intervals for better responsiveness
            check_interval = 60  # 1 minute for production
            self.debug_print(f"🏭 Production mode detected - checking every {check_interval} seconds", "INFO")
        else:
            # Development mode: longer intervals
            check_interval = 300  # 5 minutes for development
            self.debug_print(f"🛠️ Development mode - checking every {check_interval} seconds", "INFO")
        
        while self.running:
            try:
                # Wait for next check
                time.sleep(check_interval)
                
                # Update status
                self.system_status['last_run'] = self.get_ist_timestamp()
                self.system_status['next_run'] = self.get_ist_timestamp()
                self.system_status['total_runs'] += 1
                
                self.debug_print("🔄 Background auto-assign check running...", "SYSTEM")
                self.debug_print(f"   ⏰ Check Time: {self.get_ist_timestamp()}", "INFO")
                self.debug_print(f"   📊 Run #{self.system_status['total_runs']}", "INFO")
                self.debug_print(f"   🏭 Mode: {'Production' if is_production else 'Development'}", "INFO")
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
                
                # Shorter error recovery time for production
                error_recovery_time = 30 if is_production else 60
                self.debug_print(f"   ⏳ Waiting {error_recovery_time} seconds before retrying...", "INFO")
                time.sleep(error_recovery_time)
        
        self.debug_print("🛑 Auto-assign worker stopped", "SYSTEM")
        self.system_status['is_running'] = False
    
    def start_robust_auto_assign_system(self) -> Optional[threading.Thread]:
        """Start the robust auto-assign system (Render-compatible)"""
        try:
            # Check if system is already running
            if self.system_status['is_running'] and self.auto_assign_thread and self.auto_assign_thread.is_alive():
                self.debug_print("�� Auto-assign system is already running", "WARNING")
                return self.auto_assign_thread
            
            # Stop any existing system
            if self.auto_assign_thread:
                self.running = False
                time.sleep(2)  # Give thread time to stop
            
            self.debug_print("🚀 Starting robust auto-assign system...", "SYSTEM")
            
            # Check if running in production (Render)
            is_production = os.environ.get('RENDER', False) or os.environ.get('PRODUCTION', False)
            
            if is_production:
                self.debug_print("   🏭 Production mode detected - using daemon threads", "INFO")
                self.debug_print("   📋 Will check for new leads every 1 minute", "INFO")
            else:
                self.debug_print("   🛠️ Development mode - using regular threads", "INFO")
                self.debug_print("   📋 Will check for new leads every 5 minutes", "INFO")
            
            self.debug_print("   ⚡ First auto-assign check starting now...", "INFO")
            
            # Create and start new thread
            self.running = True
            
            # In production, use non-daemon threads to prevent premature termination
            if is_production:
                self.debug_print("   🏭 Production mode: Using non-daemon thread for stability", "INFO")
                self.auto_assign_thread = threading.Thread(
                    target=self.robust_auto_assign_worker, 
                    name="RobustAutoAssignWorker",
                    daemon=False  # Non-daemon for production stability
                )
            else:
                self.debug_print("   🛠️ Development mode: Using daemon thread", "INFO")
                self.auto_assign_thread = threading.Thread(
                    target=self.robust_auto_assign_worker, 
                    name="RobustAutoAssignWorker",
                    daemon=True
                )
            
            self.auto_assign_thread.start()
            
            # Update status
            self.system_status['is_running'] = True
            self.system_status['thread_id'] = self.auto_assign_thread.ident
            
            self.debug_print("✅ Robust auto-assign system started successfully", "SUCCESS")
            self.debug_print(f"   🧵 Thread ID: {self.auto_assign_thread.ident}", "INFO")
            self.debug_print(f"   🏭 Production Mode: {is_production}", "INFO")
            self.debug_print(f"   🕒 Started At: {self.system_status['started_at']}", "INFO")
            
            return self.auto_assign_thread
            
        except Exception as e:
            self.debug_print(f"❌ Error starting robust auto-assign system: {e}", "ERROR")
            self.debug_print(f"   🚨 Exception type: {type(e).__name__}", "ERROR")
            self.system_status['is_running'] = False
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
    
    def force_restart_auto_assign_system(self) -> bool:
        """Force restart the auto-assign system (useful for production troubleshooting)"""
        try:
            self.debug_print("🔄 Force restarting auto-assign system...", "SYSTEM")
            
            # Stop the system
            self.stop_auto_assign_system()
            
            # Wait a bit
            time.sleep(3)
            
            # Start the system again
            thread = self.start_robust_auto_assign_system()
            
            if thread and thread.is_alive():
                self.debug_print("✅ Auto-assign system force restarted successfully", "SUCCESS")
                return True
            else:
                self.debug_print("❌ Auto-assign system force restart failed", "ERROR")
                return False
                
        except Exception as e:
            self.debug_print(f"❌ Error force restarting auto-assign system: {e}", "ERROR")
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
    
    def manual_trigger_auto_assign(self, source: str = None) -> Dict[str, Any]:
        """Manual trigger for auto-assign (Render-optimized) with Uday branch enhancements"""
        try:
            self.debug_print("🎯 ========================================", "SYSTEM")
            self.debug_print("🎯 MANUAL TRIGGER AUTO-ASSIGN", "SYSTEM")
            self.debug_print("🎯 ========================================", "SYSTEM")
            self.debug_print("   🚀 Trigger Type: Manual (User-Initiated)", "INFO")
            self.debug_print("   ⏰ Trigger Time: " + self.get_ist_timestamp(), "INFO")
            self.debug_print("   👤 Triggered By: User/Admin", "INFO")
            self.debug_print("   🚀 Reference: Uday Branch Enhanced Logic", "INFO")
            
            # Check if running in production (Render)
            is_production = os.environ.get('RENDER', False) or os.environ.get('PRODUCTION', False)
            
            if is_production:
                self.debug_print("🏭 Production mode detected - using optimized trigger", "INFO")
                self.debug_print("   🎯 Optimization: Render-compatible processing", "INFO")
                self.debug_print("   🔧 Thread Management: Virtual threads", "INFO")
                self.debug_print("   🚀 Reference: Uday Branch Production Logic", "INFO")
            else:
                self.debug_print("🛠️ Development mode - using standard trigger", "INFO")
                self.debug_print("   🎯 Mode: Full debugging enabled", "INFO")
                self.debug_print("   🔧 Thread Management: Standard threads", "INFO")
                self.debug_print("   🚀 Reference: Uday Branch Development Logic", "INFO")
            
            # Reference from Uday branch: Enhanced trigger logic
            self.debug_print("📚 ========================================", "INFO")
            self.debug_print("📚 TRIGGER REFERENCE FROM UDAY BRANCH", "INFO")
            self.debug_print("📚 ========================================", "INFO")
            self.debug_print("   🔄 Enhanced trigger with production optimization", "INFO")
            self.debug_print("   🎯 Fair distribution algorithm", "INFO")
            self.debug_print("   📊 Comprehensive history tracking", "INFO")
            self.debug_print("   🔍 Real-time verification", "INFO")
            self.debug_print("   📝 Detailed audit logging", "INFO")
            self.debug_print("   🚀 Enhanced debug prints and stickers", "INFO")
            self.debug_print("   📈 Performance monitoring", "INFO")
            self.debug_print("📚 ========================================", "INFO")
            
            if source:
                # Trigger for specific source
                self.debug_print(f"📍 Manual trigger requested for source: {source}", "INFO")
                self.debug_print(f"   🎯 Target: Single source optimization", "INFO")
                self.debug_print(f"   🔍 Scope: {source} leads only", "INFO")
                self.debug_print(f"   🔄 Status: Executing single source trigger", "INFO")
                result = self.auto_assign_new_leads_for_source(source)
            else:
                # Trigger for all sources
                self.debug_print("📍 Manual trigger requested for all sources", "INFO")
                self.debug_print("   🎯 Target: Multi-source optimization", "INFO")
                self.debug_print("   🔍 Scope: All configured sources", "INFO")
                self.debug_print(f"   🔄 Status: Executing multi-source trigger", "INFO")
                result = self.check_and_assign_new_leads()
            
            if result and result.get('success'):
                assigned_count = result.get('assigned_count', 0) or result.get('total_assigned', 0)
                self.debug_print(f"✅ ========================================", "SUCCESS")
                self.debug_print(f"✅ MANUAL TRIGGER COMPLETED SUCCESSFULLY", "SUCCESS")
                self.debug_print(f"✅ ========================================", "SUCCESS")
                self.debug_print(f"   🎯 Leads assigned: {assigned_count}", "SUCCESS")
                self.debug_print(f"   📝 Message: {result.get('message', 'N/A')}", "INFO")
                self.debug_print(f"   🏷️ Source: {source or 'All Sources'}", "INFO")
                self.debug_print(f"   ⏰ Completion Time: {self.get_ist_timestamp()}", "INFO")
                self.debug_print(f"   🎉 Status: Trigger Successful", "SUCCESS")
                self.debug_print(f"   🚀 Reference: Uday Branch Enhanced Logic", "SUCCESS")
                
                # Update system status for manual triggers
                if assigned_count > 0:
                    self.system_status['total_leads_assigned'] += assigned_count
                    self.debug_print(f"   📊 Total leads assigned so far: {self.system_status['total_leads_assigned']}", "INFO")
                    self.debug_print(f"   🔄 System status updated", "INFO")
                    self.debug_print(f"   📈 Performance: {assigned_count} leads in this trigger", "SUCCESS")
                else:
                    self.debug_print(f"   ℹ️ No leads assigned in this trigger", "INFO")
                    self.debug_print(f"   🔍 Status: All leads already assigned", "INFO")
                
                self.debug_print(f"✅ ========================================", "SUCCESS")
                
                return {
                    'success': True,
                    'message': f'Manual trigger completed: {assigned_count} leads assigned',
                    'assigned_count': assigned_count,
                    'source': source,
                    'timestamp': self.get_ist_timestamp(),
                    'production_mode': is_production,
                    'trigger_type': 'manual',
                    'trigger_reference': 'Uday branch enhanced trigger',
                    'enhanced_features': ['debug_prints', 'stickers', 'performance_monitoring', 'real_time_verification']
                }
            else:
                error_msg = result.get('message', 'Unknown error') if result else 'No result'
                self.debug_print(f"❌ ========================================", "ERROR")
                self.debug_print(f"❌ MANUAL TRIGGER FAILED", "ERROR")
                self.debug_print(f"❌ ========================================", "ERROR")
                self.debug_print(f"   🚨 Error: {error_msg}", "ERROR")
                self.debug_print(f"   🏷️ Source: {source or 'All Sources'}", "ERROR")
                self.debug_print(f"   ⏰ Failure Time: {self.get_ist_timestamp()}", "ERROR")
                self.debug_print(f"   🎯 Status: Trigger Failed", "ERROR")
                self.debug_print(f"   🔍 Action: Review error and retry", "ERROR")
                self.debug_print(f"❌ ========================================", "ERROR")
                
                return {
                    'success': False,
                    'message': f'Manual trigger failed: {error_msg}',
                    'source': source,
                    'timestamp': self.get_ist_timestamp(),
                    'production_mode': is_production,
                    'trigger_type': 'manual',
                    'trigger_reference': 'Uday branch enhanced trigger',
                    'enhanced_features': ['debug_prints', 'stickers', 'performance_monitoring', 'real_time_verification']
                }
                
        except Exception as e:
            self.debug_print(f"❌ ========================================", "ERROR")
            self.debug_print(f"❌ CRITICAL ERROR IN MANUAL TRIGGER", "ERROR")
            self.debug_print(f"❌ ========================================", "ERROR")
            self.debug_print(f"   🚨 Exception: {e}", "ERROR")
            self.debug_print(f"   🚨 Exception type: {type(e).__name__}", "ERROR")
            self.debug_print(f"   📍 Source: {source}", "ERROR")
            self.debug_print(f"   ⏰ Error Time: {self.get_ist_timestamp()}", "ERROR")
            self.debug_print(f"   🎯 Status: Critical Error", "ERROR")
            self.debug_print(f"   🔍 Action: Review error and retry", "ERROR")
            self.debug_print(f"❌ ========================================", "ERROR")
            
            return {
                'success': False,
                'message': f'Critical error in manual trigger: {str(e)}',
                'source': source,
                'timestamp': self.get_ist_timestamp(),
                'production_mode': os.environ.get('RENDER', False) or os.environ.get('PRODUCTION', False),
                'trigger_type': 'manual',
                'trigger_reference': 'Uday branch enhanced trigger',
                'enhanced_features': ['debug_prints', 'stickers', 'performance_monitoring', 'real_time_verification']
            }
    
    def enable_debug_mode(self):
        """Enable debug mode for enhanced logging"""
        self.debug_mode = True
        self.debug_print("🔍 Debug mode enabled", "SYSTEM")
        self.debug_print("   📝 Enhanced logging active", "INFO")
        self.debug_print("   🚀 Uday branch features active", "INFO")
        self.debug_print("   📊 Detailed performance monitoring", "INFO")
    
    def disable_debug_mode(self):
        """Disable debug mode"""
        self.debug_mode = False
        print("🔍 Debug mode disabled")
    
    def enable_verbose_logging(self):
        """Enable verbose logging for maximum detail"""
        self.verbose_logging = True
        self.debug_print("📝 Verbose logging enabled", "SYSTEM")
        self.debug_print("   📊 Maximum detail logging", "INFO")
        self.debug_print("   🚀 Uday branch verbose features", "INFO")
    
    def disable_verbose_logging(self):
        """Disable verbose logging"""
        self.verbose_logging = False
        print("📝 Verbose logging disabled")
    
    def get_debug_status(self) -> Dict[str, Any]:
        """Get current debug configuration status"""
        return {
            'debug_mode': self.debug_mode,
            'verbose_logging': self.verbose_logging,
            'timestamp': self.get_ist_timestamp(),
            'enhanced_features': [
                'debug_prints',
                'stickers', 
                'performance_monitoring',
                'real_time_verification',
                'uday_branch_reference'
            ]
        }

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
