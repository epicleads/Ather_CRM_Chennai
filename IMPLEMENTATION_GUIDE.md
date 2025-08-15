# Lead Approval System Implementation Guide

## Overview
This system implements a double verification workflow for leads where Product Specialists (PS) must get Branch Head approval before marking leads as "Won" when the status is "Booked" or "Retailed".

## System Flow

### 1. PS Lead Update Process
1. **PS selects lead status as "Booked" or "Retailed"**
2. **System automatically:**
   - Sets final status to "Waiting for Approval"
   - Requires 8-digit Order ID input
   - Locks follow-up date
   - Sets approval status to "Waiting for Approval"
3. **PS enters 8-digit Order ID (numbers only)**
4. **Lead is submitted for Branch Head approval**

### 2. Branch Head Approval Process
1. **Branch Head sees leads in approval dashboard**
2. **Reviews lead details including Order ID**
3. **Clicks "Approve" button**
4. **System automatically:**
   - Changes final status to "Won"
   - Records approval timestamp
   - Records who approved it
   - Updates all related tables

## Database Changes

### New Fields Added
All lead tables now include these fields:
- `order_id` VARCHAR(8) - 8-digit order number
- `approval_status` VARCHAR(50) - Pending, Waiting for Approval, Approved, Rejected
- `approval_requested_at` TIMESTAMP - When approval was requested
- `approved_by` VARCHAR(100) - Branch Head who approved
- `approved_at` TIMESTAMP - When approved
- `approval_remarks` TEXT - Optional approval notes

### Tables Modified
- `ps_followup_master`
- `lead_master`
- `activity_leads`
- `walkin_table`

### Database View Created
- `branch_approval_leads` - Shows all leads waiting for approval across all tables

## Implementation Steps

### Step 1: Database Setup
```bash
# Run the SQL script to add new fields and create view
psql -d your_database -f approval_system_sql.sql
```

### Step 2: Code Changes Applied
1. **PS Lead Update Template** (`templates/update_ps_lead.html`)
   - Added Order ID input field
   - Modified JavaScript to handle approval workflow
   - Added validation for 8-digit Order ID

2. **Branch Head Dashboard** (`templates/bh_dashboard.html`)
   - Added approval section
   - Added approval table with actions
   - Added JavaScript functions for approval workflow

3. **Flask App** (`app.py`)
   - Added new API endpoints for approval system
   - Modified PS lead update logic
   - Added approval workflow handling

### Step 3: Testing the System

#### Test PS Workflow
1. Login as PS user
2. Update a lead status to "Booked" or "Retailed"
3. Verify Order ID field appears and is required
4. Enter 8-digit Order ID
5. Submit - status should become "Waiting for Approval"

#### Test Branch Head Workflow
1. Login as Branch Head
2. Check approval dashboard
3. Verify lead appears in approval list
4. Click "View" to see details
5. Click "Approve" to approve lead
6. Verify status changes to "Won"

## API Endpoints

### 1. Get Approval Leads
```
GET /api/bh_approval_leads
```
Returns all leads waiting for approval in the Branch Head's branch.

### 2. Approve Lead
```
POST /api/bh_approve_lead
Body: {
    "source_table": "ps_followup",
    "lead_id": "lead_uid",
    "remarks": "Optional approval notes"
}
```

### 3. Get Lead Details
```
GET /api/bh_lead_details?source_table=ps_followup&lead_id=lead_uid
```

## Security Features

1. **Access Control**: Only Branch Head users can access approval endpoints
2. **Branch Isolation**: Branch Head can only see/approve leads from their branch
3. **Audit Logging**: All approval actions are logged with user details
4. **Input Validation**: Order ID must be exactly 8 digits (numbers only)

## Validation Rules

### Order ID Requirements
- Must be exactly 8 characters
- Only numbers allowed (0-9)
- Required when status is "Booked" or "Retailed"
- Hidden for other statuses

### Status Transitions
- **Booked/Retailed** → **Waiting for Approval** (requires Order ID)
- **Waiting for Approval** → **Won** (after Branch Head approval)
- **Other statuses** → **Normal workflow** (no approval required)

## Error Handling

### Common Scenarios
1. **Invalid Order ID**: Form validation prevents submission
2. **Missing Order ID**: Required field validation
3. **Database Errors**: Graceful error messages
4. **Access Denied**: Clear permission error messages

### User Feedback
- Success messages for approvals
- Error messages for failures
- Loading states during operations
- Confirmation dialogs for approvals

## Monitoring and Maintenance

### Audit Trail
- All approval actions are logged
- Includes user, timestamp, and details
- Can be reviewed in audit logs

### Performance Considerations
- Database indexes on approval fields
- Efficient queries for approval lists
- Caching for frequently accessed data

## Troubleshooting

### Common Issues
1. **Order ID not appearing**: Check JavaScript console for errors
2. **Approval not working**: Verify user has Branch Head permissions
3. **Leads not showing**: Check branch assignment and approval status
4. **Database errors**: Verify all new fields were added correctly

### Debug Steps
1. Check browser console for JavaScript errors
2. Verify database schema changes
3. Check user permissions and session data
4. Review server logs for API errors

## Future Enhancements

### Potential Improvements
1. **Email Notifications**: Alert Branch Head of new approvals needed
2. **Bulk Approvals**: Approve multiple leads at once
3. **Approval History**: Track all approval actions over time
4. **Mobile Support**: Mobile-friendly approval interface
5. **Auto-approval Rules**: Automatic approval for certain conditions

### Configuration Options
1. **Approval Thresholds**: Set minimum requirements for auto-approval
2. **Escalation Rules**: Auto-escalate to higher management
3. **Approval Workflows**: Multi-level approval processes
4. **Custom Fields**: Additional approval requirements

## Support and Maintenance

### Regular Tasks
1. **Monitor approval queue**: Check for stuck approvals
2. **Review audit logs**: Ensure proper approval patterns
3. **Update approval rules**: Modify as business needs change
4. **Performance monitoring**: Track approval processing times

### User Training
1. **PS Users**: How to submit leads for approval
2. **Branch Heads**: How to review and approve leads
3. **Administrators**: How to monitor and maintain the system

## Conclusion

This approval system provides a robust, secure, and user-friendly way to implement double verification for high-value leads. The system maintains data integrity while providing clear audit trails and efficient workflows for all users involved.
