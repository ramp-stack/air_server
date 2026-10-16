#!/usr/bin/env python3
"""Run on the new host after bootstrap. Does not start the database server."""
import json, os, pathlib, subprocess
def write(path, contents, mode=0o644):
    p=pathlib.Path(path); p.parent.mkdir(parents=True,exist_ok=True)
    p.write_text(contents.lstrip('\n')); p.chmod(mode)
def run(*cmd): subprocess.run(cmd,check=True)
env=dict(line.split('=',1) for line in pathlib.Path('/etc/air/infrastructure.env').read_text().splitlines())
bucket=env['AIR_BACKUP_BUCKET']
write('/etc/systemd/system/air-server.service', '''
[Unit]
Description=Air production single-writer server
After=network-online.target
Wants=network-online.target
RequiresMountsFor=/var/lib/air
StartLimitIntervalSec=0
[Service]
User=air
Group=air
WorkingDirectory=/var/lib/air
Environment=AIR_STORAGE=/var/lib/air/STORAGE.db
Environment=AIR_SECRET_FILE=/etc/air/identity.json
Environment=AIR_WS_ADDR=127.0.0.1:5703
Environment=AIR_HEALTH_ADDR=127.0.0.1:5701
ExecStartPre=/usr/bin/mountpoint -q /var/lib/air
ExecStartPre=/usr/bin/test -s /var/lib/air/STORAGE.db
ExecStart=/opt/air/bin/air_server
Restart=always
RestartSec=2
TimeoutStopSec=30
KillSignal=SIGTERM
UMask=0077
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/air /var/log/air
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictSUIDSGID=true
LockPersonality=true
CapabilityBoundingSet=
LimitNOFILE=8192
TasksMax=128
MemoryMax=3G
StandardOutput=append:/var/log/air/server.log
StandardError=append:/var/log/air/server.log
[Install]
WantedBy=multi-user.target
''')
write('/etc/litestream.yml', f'''
region: us-east-1
sync-interval: 1s
snapshot:
  interval: 24h
  retention: 840h
socket:
  enabled: true
  path: /run/litestream/control.sock
  permissions: 0600
dbs:
  - path: /var/lib/air/STORAGE.db
    replica:
      url: s3://{bucket}/sqlite/production
''',0o644)
write('/etc/systemd/system/litestream.service', '''
[Unit]
Description=Continuous SQLite backup to encrypted S3
After=network-online.target air-server.service
Wants=network-online.target air-server.service
RequiresMountsFor=/var/lib/air
StartLimitIntervalSec=0
[Service]
User=air
Group=air
Environment=AWS_REGION=us-east-1
Environment=AWS_DEFAULT_REGION=us-east-1
ExecStartPre=/usr/bin/test -s /var/lib/air/STORAGE.db
ExecStart=/usr/local/bin/litestream replicate -config /etc/litestream.yml
Restart=always
RestartSec=5
RuntimeDirectory=litestream
RuntimeDirectoryMode=0700
UMask=0077
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/air /var/log/air /run/litestream
CapabilityBoundingSet=
MemoryMax=512M
StandardOutput=append:/var/log/air/litestream.log
StandardError=append:/var/log/air/litestream.log
[Install]
WantedBy=multi-user.target
''')
write('/etc/nginx/conf.d/air.conf','''
map $http_upgrade $air_connection_upgrade {
    default upgrade;
    '' close;
}
limit_conn_zone $binary_remote_addr zone=air_per_ip:10m;
server {
    listen 80;
    server_name air.orange.me;
    location /.well-known/acme-challenge/ { root /var/www/letsencrypt; }
    location / { return 301 https://air.orange.me$request_uri; }
}
server {
    listen 443 ssl;
    server_name air.orange.me;
    ssl_certificate /etc/letsencrypt/live/air.orange.me/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/air.orange.me/privkey.pem;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_session_cache shared:air_tls:10m;
    ssl_session_timeout 1d;
    ssl_session_tickets off;
    server_tokens off;
    add_header Strict-Transport-Security "max-age=31536000" always;
    client_max_body_size 16m;
    client_header_timeout 10s;
    client_body_timeout 15s;
    keepalive_timeout 30s;
    location = /healthz {
        proxy_pass http://127.0.0.1:5701;
        proxy_connect_timeout 2s;
        proxy_read_timeout 5s;
    }
    location / {
        limit_conn air_per_ip 32;
        proxy_pass http://127.0.0.1:5703;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection $air_connection_upgrade;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $remote_addr;
        proxy_set_header X-Forwarded-Proto https;
        proxy_connect_timeout 3s;
        proxy_read_timeout 3600s;
        proxy_send_timeout 30s;
        proxy_buffering off;
    }
}
server {
    listen 5702;
    server_name air.orange.me;
    server_tokens off;
    client_header_timeout 10s;
    location / {
        limit_conn air_per_ip 32;
        proxy_pass http://127.0.0.1:5703;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection $air_connection_upgrade;
        proxy_set_header Host $host;
        proxy_connect_timeout 3s;
        proxy_read_timeout 3600s;
        proxy_send_timeout 30s;
        proxy_buffering off;
    }
}
''')
write('/etc/systemd/system/air-certificate-renew.service','''
[Unit]
Description=Renew Air TLS certificate
[Service]
Type=oneshot
ExecStart=/usr/bin/certbot renew --quiet --no-random-sleep-on-renew --deploy-hook "/usr/bin/systemctl reload nginx"
''')
write('/etc/systemd/system/nginx.service.d/restart.conf','''
[Service]
Restart=on-failure
RestartSec=2
''')
write('/etc/systemd/system/air-certificate-renew.timer','''
[Unit]
Description=Check Air certificate twice daily
[Timer]
OnCalendar=*-*-* 03,15:00:00
RandomizedDelaySec=3600
Persistent=true
[Install]
WantedBy=timers.target
''')
write('/usr/local/sbin/air-monitor', '''
#!/usr/bin/python3
import json, math, os, pathlib, shutil, subprocess, time, datetime
os.environ['AWS_DEFAULT_REGION']='us-east-1'
env=dict(line.split('=',1) for line in pathlib.Path('/etc/air/infrastructure.env').read_text().splitlines())
def aws(*args):
    raw=subprocess.check_output(['aws',*args,'--output','json'],timeout=30,text=True)
    return json.loads(raw) if raw.strip() else {}
healthy=0
try:
    r=subprocess.run(['curl','--fail','--silent','--max-time','5','--resolve','air.orange.me:443:127.0.0.1','https://air.orange.me/healthz'],stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=7)
    healthy=int(r.returncode==0 and r.stdout==b'ok\\n')
except Exception: pass
certificate_days=0
try:
    expiry=subprocess.check_output(['openssl','x509','-enddate','-noout','-in','/etc/letsencrypt/live/air.orange.me/cert.pem'],text=True,timeout=5).strip().split('=',1)[1]
    when=datetime.datetime.strptime(expiry,'%b %d %H:%M:%S %Y %Z').replace(tzinfo=datetime.timezone.utc)
    certificate_days=(when-datetime.datetime.now(datetime.timezone.utc)).total_seconds()/86400
except Exception: pass
failures=pathlib.Path('/run/air-health-failures')
count=0 if healthy else (int(failures.read_text())+1 if failures.exists() else 1)
failures.write_text(str(count))
if count>=3:
    subprocess.run(['systemctl','restart','air-server'],timeout=40)
    failures.write_text('0')
backup=0
try:
    result=subprocess.run(['/usr/local/bin/litestream','sync','-wait','-timeout','25','-socket','/run/litestream/control.sock','/var/lib/air/STORAGE.db'],timeout=30,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    backup=int(result.returncode==0)
    if not backup: print('Backup sync failed:', result.stderr.decode()[:500],flush=True)
except Exception as e: print('Backup sync error:',str(e),flush=True)
usage=shutil.disk_usage('/var/lib/air'); used=100*usage.used/usage.total
metrics=[{'MetricName':name,'Dimensions':[{'Name':'Service','Value':'air-production'}],'Value':value} for name,value in [('ApplicationHealthy',healthy),('BackupHealthy',backup),('DiskUsedPercent',used),('CertificateDaysRemaining',certificate_days)]]
aws('cloudwatch','put-metric-data','--namespace','AirServer','--metric-data',json.dumps(metrics))
volume=env['AIR_DATA_VOLUME']
try:
    modifications=aws('ec2','describe-volumes-modifications','--filters','Name=volume-id,Values='+volume)['VolumesModifications']
    state=modifications[0]['ModificationState'] if modifications else None
    if state in ('optimizing','completed'):
        subprocess.run(['xfs_growfs','/var/lib/air'],check=True,timeout=30,stdout=subprocess.DEVNULL)
    if used>=70 and state not in ('modifying','optimizing'):
        size=aws('ec2','describe-volumes','--volume-ids',volume)['Volumes'][0]['Size']
        if size<500:
            target=min(500,math.ceil(size*1.25))
            aws('ec2','modify-volume','--volume-id',volume,'--size',str(target))
            print(f'Data disk growth requested: {size} -> {target} GiB',flush=True)
        else: print('Data disk reached 500 GiB automatic growth limit',flush=True)
except Exception as e: print('Disk growth check failed:',str(e),flush=True)
print(json.dumps({'healthy':healthy,'backup':backup,'disk_used_percent':round(used,2)}),flush=True)
''',0o755)
write('/etc/systemd/system/air-monitor.service','''
[Unit]
Description=Air health, backup, and storage monitoring
After=network-online.target
RequiresMountsFor=/var/lib/air
[Service]
Type=oneshot
ExecStart=/usr/local/sbin/air-monitor
TimeoutStartSec=180
StandardOutput=append:/var/log/air/monitor.log
StandardError=append:/var/log/air/monitor.log
''')
write('/etc/systemd/system/air-monitor.timer','''
[Unit]
Description=Monitor Air every minute
[Timer]
OnBootSec=2min
OnUnitActiveSec=60s
AccuracySec=5s
[Install]
WantedBy=timers.target
''')
write('/etc/logrotate.d/air','''
/var/log/air/*.log {
    daily
    rotate 14
    size 20M
    missingok
    notifempty
    compress
    delaycompress
    copytruncate
    su air air
}
''')
write('/opt/aws/amazon-cloudwatch-agent/etc/air.json',json.dumps({'logs':{'logs_collected':{'files':{'collect_list':[{'file_path':f'/var/log/air/{name}.log','log_group_name':'/air/production','log_stream_name':'{instance_id}/'+name} for name in ['server','litestream','monitor']]}}}}))
pathlib.Path('/var/www/letsencrypt').mkdir(parents=True,exist_ok=True)
run('chown','root:air','/etc/air')
run('systemctl','daemon-reload')
print('Production configuration written; services remain stopped until data and TLS are ready.')
