# Durable jobs

Takopi agent runs are one-shot subprocesses. A background task created inside
Claude Code is stopped when that subprocess exits, so it cannot reliably wait
for a deploy and report back later.

Use a durable job for work that must continue after the agent run finishes:

```bash
cat > /tmp/wait-for-deploy.sh <<'SH'
#!/bin/bash
set -euo pipefail

# Wait, verify, and perform only the actions authorized in the current request.
until deploy_is_complete; do
  sleep 30
done
verify_deploy
echo "Deploy completed and verification passed."
SH

takopi jobs start deploy-v044-$(date +%s) \
  --script /tmp/wait-for-deploy.sh \
  --chat-id 123456789 \
  --timeout 3600 \
  --title "deploy v0.4.4"
```

Takopi copies the script into `~/.takopi/jobs/<id>/`, starts it in a transient
systemd unit, records its state and output, and sends the final result through
the configured Telegram bot. The job survives the originating Claude run,
Takopi restarts, and SSH disconnects.

Inspect or stop jobs with:

```bash
takopi jobs list
takopi jobs status deploy-v044-1234567890
takopi jobs cancel deploy-v044-1234567890
```

Durable execution does not broaden authorization. Scripts should contain only
actions already authorized by the user. Destructive or externally visible
follow-up actions still require explicit permission.
