# Ather CRM Chennai - Production Deployment Guide

## 🚀 Fixed Issues

This deployment addresses the following critical production issues:

1. **✅ Eventlet SSL Monkey Patching Conflict** - Fixed by completely removing eventlet dependency
2. **✅ Import Order Issues** - Fixed by removing eventlet imports and monkey patching
3. **✅ Flask Application Context Issues** - Fixed by wrapping background tasks in `app.app_context()`
4. **✅ Gunicorn Configuration Issues** - Fixed by switching to sync workers instead of eventlet
5. **✅ Flask-SocketIO Compatibility** - Fixed by replacing with regular threading for background tasks

## 🔧 Deployment Steps

### 1. **Deploy to Render**

The application is now configured to work with Render's sync workers:

```bash
# Render will automatically use the configuration from render.yaml
# Start command: gunicorn -w 2 --timeout 120 --bind 0.0.0.0:$PORT app:app
```

### 2. **Environment Variables**

Ensure these environment variables are set in Render:

```bash
RENDER=true
PRODUCTION=true
PYTHONHTTPSVERIFY=0
CURL_CA_BUNDLE=""
REQUESTS_CA_BUNDLE=""
SSL_CERT_FILE=""
```

### 3. **Auto-Assign Configuration**

In production, auto-assign is triggered via HTTP endpoints instead of background threads:

- **Manual Trigger**: `GET /api/auto_assign_trigger`
- **Health Check**: `GET /health`
- **Status**: `GET /api/status`

### 4. **External Scheduling (Recommended)**

Set up external monitoring to trigger auto-assign every 5 minutes:

```bash
# Cron job example
*/5 * * * * curl 'https://your-domain.com/api/auto_assign_trigger'

# UptimeRobot: Monitor /api/auto_assign_trigger every 5 minutes
# Render Cron: Add cron job to call /api/auto_assign_trigger
```

## 🧪 Testing

### Local Testing

```bash
# Test with sync workers locally
gunicorn -w 2 --timeout 120 --bind 127.0.0.1:5000 app:app

# Test with Flask development server
python app.py
```

### Production Testing

1. **Health Check**: `GET /health`
2. **Auto-Assign**: `GET /api/auto_assign_trigger`
3. **Status**: `GET /api/status`

## 📊 Monitoring

- **Health Endpoint**: `/health` - Database connectivity and overall health
- **Status Endpoint**: `/api/status` - Application status and environment info
- **Auto-Assign**: `/api/auto_assign_trigger` - Manual auto-assign trigger

## 🔍 Troubleshooting

### If SSL errors persist:

1. Check environment variables are set correctly
2. Verify `PYTHONHTTPSVERIFY=0` is set
3. Ensure sync workers are being used (not eventlet)

### If context errors persist:

1. Verify all background operations use `with app.app_context():`
2. Check that threading is properly imported
3. Ensure production auto-assign uses HTTP endpoints

## ✅ Success Indicators

- No more `TypeError: super(type, obj): obj must be an instance or subtype of type`
- No more `RuntimeError: Working outside of application context`
- No more eventlet monkey patching warnings
- Successful database connections and operations
- Auto-assign working via HTTP endpoints
- Background tasks using regular threading instead of SocketIO

## 🎯 Next Steps

1. Deploy to Render using the updated configuration
2. Set up external monitoring for auto-assign
3. Monitor logs for any remaining issues
4. Test all functionality in production environment

## 🔄 Changes Made

### Removed Dependencies:
- `eventlet` - Complete removal to avoid SSL conflicts
- `flask-socketio` - Replaced with regular threading

### Updated Code:
- Background tasks now use `threading.Thread()` instead of `socketio.start_background_task()`
- All eventlet references removed from status endpoints
- Production environment detection updated for sync workers
- Auto-assign status now shows 'http-triggered' instead of 'eventlet-compatible'
